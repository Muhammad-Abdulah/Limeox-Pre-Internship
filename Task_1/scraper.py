"""
Tiered, resumable news scraper -> PostgreSQL.

  python scraper.py init              create tables
  python scraper.py load sources.txt  load domains (one per line)
  python scraper.py check             Tier 0: health check every source
  python scraper.py discover          Tier 1: collect article URLs (sitemaps + RSS)
  python scraper.py fetch             Tier 2: fast HTTP fetch + extraction
  python scraper.py render            Tier 3: Playwright for JS-only pages
  python scraper.py stats             progress

All state lives in Postgres, so you can Ctrl+C any command and re-run it:
it continues where it stopped.

Install:
  pip install asyncpg httpx[http2] trafilatura lxml python-dateutil playwright
  playwright install chromium
"""
import asyncio, gzip, json, os, random, re, sys, time
from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib import robotparser

import asyncpg, httpx, trafilatura
from dateutil import parser as dtparse
from lxml import etree

DSN = os.getenv("DB_DSN", "postgresql://postgres:postgres@localhost:5432/news")
CONCURRENCY = int(os.getenv("CONCURRENCY", "60"))        # total in-flight requests
PER_DOMAIN = int(os.getenv("PER_DOMAIN", "2"))           # parallel requests per site
DOMAIN_DELAY = float(os.getenv("DOMAIN_DELAY", "1.0"))   # min seconds between hits to one site
RENDER_WORKERS = int(os.getenv("RENDER_WORKERS", "4"))
PROXY_FILE = os.getenv("PROXY_FILE", "proxies.txt")      # optional, one proxy URL per line
RESPECT_ROBOTS = os.getenv("RESPECT_ROBOTS", "1") == "1"
MAX_URLS_PER_SOURCE = int(os.getenv("MAX_URLS_PER_SOURCE", "20000"))

TZINFOS = {"PST": -8*3600, "PDT": -7*3600, "MST": -7*3600, "MDT": -6*3600,
          "CST": -6*3600, "CDT": -5*3600, "EST": -5*3600, "EDT": -4*3600,
          "ET": -5*3600, "PT": -8*3600, "CT": -6*3600, "MT": -7*3600}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9",
           "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}

BLOCK_MARKERS = ("captcha", "are you a robot", "unusual traffic", "access denied",
                 "cf-challenge", "attention required", "verify you are human", "px-captcha")

# ----------------------------------------------------------------- helpers
def load_proxies():
    if os.path.exists(PROXY_FILE):
        return [l.strip() for l in open(PROXY_FILE) if l.strip()]
    return []
PROXIES = load_proxies()

def pick_proxy():
    return random.choice(PROXIES) if PROXIES else None

def make_client(proxy=None):
    return httpx.AsyncClient(headers=HEADERS, timeout=httpx.Timeout(20.0, connect=8.0),
                             follow_redirects=True, http2=True, proxy=proxy,
                             limits=httpx.Limits(max_connections=CONCURRENCY * 2,
                                                 max_keepalive_connections=CONCURRENCY * 2,
                                                 keepalive_expiry=60))

def looks_blocked(status, text):
    if status in (401, 403, 429, 503):
        return True
    low = text[:6000].lower()
    return len(text) < 20000 and any(m in low for m in BLOCK_MARKERS)

class DomainGate:
    """Per-domain concurrency + minimum spacing, so no single site gets hammered."""
    def __init__(self):
        self.sems, self.last = {}, {}
    async def wait(self, domain):
        sem = self.sems.setdefault(domain, asyncio.Semaphore(PER_DOMAIN))
        await sem.acquire()
        gap = DOMAIN_DELAY + random.uniform(0, 0.6) - (time.monotonic() - self.last.get(domain, 0))
        if gap > 0:
            await asyncio.sleep(gap)
        self.last[domain] = time.monotonic()
        return sem

robots_cache = {}
async def robots_allows(client, domain, url):
    if not RESPECT_ROBOTS:
        return True
    rp = robots_cache.get(domain)
    if rp is None:
        rp = robotparser.RobotFileParser()
        try:
            r = await client.get(f"https://{domain}/robots.txt")
            rp.parse(r.text.splitlines() if r.status_code == 200 else [])
        except Exception:
            rp.parse([])
        robots_cache[domain] = rp
    return rp.can_fetch(UA, url)

async def cooldown(pool, domain, minutes):
    await pool.execute(
        """UPDATE sources SET fail_streak = fail_streak + 1,
           cooldown_until = now() + make_interval(mins => $2)
           WHERE domain = $1 AND (cooldown_until IS NULL OR cooldown_until < now())""",
        domain, minutes)

# ------------------------------------------------------------ extraction
META_DATE = re.compile(
    r'(?:article:published_time|datePublished|og:article:published_time|pubdate)["\']?'
    r'[^>]*?content=["\']([^"\']+)', re.I)
LD_DATE = re.compile(r'"datePublished"\s*:\s*"([^"]+)"')

LD_BLOCK = re.compile(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I)
_AN = r'(?:author|article:author|parsely-author|byline|twitter:creator|dc\.creator|sailthru\.author)'
META_AUTHOR = re.compile(r'<meta[^>]+(?:name|property)=["\']' + _AN + r'["\'][^>]*?content=["\']([^"\']+)["\']', re.I)
META_AUTHOR_REV = re.compile(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]*?(?:name|property)=["\']' + _AN + r'["\']', re.I)

def _names(node):
    """Collect author names from a JSON-LD 'author' value (str, dict or list)."""
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [node["name"]] if isinstance(node.get("name"), str) else []
    if isinstance(node, list):
        return [n for x in node for n in _names(x)]
    return []

def _walk(node, out):
    if isinstance(node, dict):
        if "author" in node:
            out += _names(node["author"])
        for v in node.values():
            _walk(v, out)
    elif isinstance(node, list):
        for v in node:
            _walk(v, out)

def find_author(html):
    names = []
    for block in LD_BLOCK.findall(html):
        try:
            _walk(json.loads(block.strip()), names)
        except Exception:
            pass
    if not names:
        names = META_AUTHOR.findall(html) + META_AUTHOR_REV.findall(html)
    clean = []
    for n in names:
        n = re.sub(r"\s+", " ", n).strip()
        if n and not n.lower().startswith("http") and n not in clean and len(n) < 80:
            clean.append(n)
    return ", ".join(clean[:5]) or None

def extract(html, url, fast=False):
    kw = dict(url=url, output_format="json", with_metadata=True, include_comments=False)
    if fast:
        kw["fast"] = True            # skip slow fallback extractors (bulk CC-NEWS mode)
    else:
        kw["favor_recall"] = True
    try:
        raw = trafilatura.extract(html, **kw)
    except TypeError:                # older trafilatura without 'fast'
        kw.pop("fast", None)
        raw = trafilatura.extract(html, **kw)
    if not raw:
        return None
    try:
        d = json.loads(raw)
    except ValueError:
        try:
            d = json.loads(raw, strict=False)
        except ValueError:
            return None            # one malformed page must never kill a whole job
    published = None
    for rx in (LD_DATE, META_DATE):          # prefer full timestamps (date + time)
        m = rx.search(html)
        if m:
            try:
                published = dtparse.parse(m.group(1), tzinfos=TZINFOS); break
            except Exception:
                pass
    if published is None and d.get("date"):
        try: published = dtparse.parse(d["date"], tzinfos=TZINFOS)
        except Exception: pass
    if published is not None and published.tzinfo is None:
        published = published.replace(tzinfo=timezone.utc)
    return dict(headline=(d.get("title") or "").strip(),
                content=(d.get("text") or "").strip(),
                author=(d.get("author") or "").strip() or find_author(html),
                published_at=published)

EN_WORDS = {"the", "and", "of", "to", "in", "is", "that", "for", "it", "with", "as", "was",
            "on", "are", "said", "by", "at", "from", "this", "have", "has", "be", "his", "her"}

def is_english(text):
    w = re.findall(r"[a-zA-Z']+", text[:2000].lower())
    if len(w) < 40:
        return False
    return sum(x in EN_WORDS for x in w) / len(w) >= 0.18

def good(a):
    return a and a["headline"] and len(a["content"]) >= 300

async def save_article(pool, uid, url, domain, a, method):
    async with pool.acquire() as c, c.transaction():
        await c.execute(
            """INSERT INTO articles (url, domain, headline, content, author, published_at, method)
               VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (url) DO NOTHING""",
            url, domain, a["headline"], a["content"], a["author"], a["published_at"], method)
        await c.execute("UPDATE urls SET status='done', locked_at=NULL WHERE id=$1", uid)

# ------------------------------------------------------------------ init/load
async def cmd_init(pool):
    await pool.execute(open(os.path.join(os.path.dirname(__file__), "schema.sql")).read())
    print("tables ready")

async def cmd_load(pool, path):
    n = 0
    for line in open(path):
        d = line.strip().lower().replace("https://", "").replace("http://", "").strip("/")
        if d:
            await pool.execute("INSERT INTO sources(domain) VALUES($1) ON CONFLICT DO NOTHING", d)
            n += 1
    print(f"loaded {n} sources")

# --------------------------------------------------------- Tier 0: health check
async def check_one(pool, client, gate, domain):
    sem = await gate.wait(domain)
    try:
        if not await robots_allows(client, domain, f"https://{domain}/"):
            await pool.execute("UPDATE sources SET status='disallowed', last_check=now() WHERE domain=$1", domain)
            return
        r = await client.get(f"https://{domain}/")
        if looks_blocked(r.status_code, r.text):
            await pool.execute("UPDATE sources SET status='blocked', last_check=now(), note=$2 WHERE domain=$1",
                               domain, f"HTTP {r.status_code}")
        elif r.status_code >= 400:
            await pool.execute("UPDATE sources SET status='dead', last_check=now(), note=$2 WHERE domain=$1",
                               domain, f"HTTP {r.status_code}")
        else:
            await pool.execute("UPDATE sources SET status='ok', last_check=now(), note=NULL WHERE domain=$1", domain)
    except Exception as e:
        await pool.execute("UPDATE sources SET status='dead', last_check=now(), note=$2 WHERE domain=$1",
                           domain, str(e)[:200])
    finally:
        sem.release()

async def cmd_check(pool):
    rows = await pool.fetch("SELECT domain FROM sources WHERE status IN ('new','blocked','dead')")
    gate = DomainGate()
    async with make_client(pick_proxy()) as client:
        outer = asyncio.Semaphore(CONCURRENCY)
        async def run(d):
            async with outer:
                await check_one(pool, client, gate, d)
        await asyncio.gather(*(run(r["domain"]) for r in rows))
    for r in await pool.fetch("SELECT status, count(*) FROM sources GROUP BY 1"):
        print(r["status"], r["count"])

# ---------------------------------------------------- Tier 1: URL discovery
async def get_xml(client, url):
    r = await client.get(url)
    if r.status_code != 200:
        return None
    data = r.content
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    try:
        return etree.fromstring(data, parser=etree.XMLParser(recover=True, huge_tree=True))
    except Exception:
        return None

def local(tag):
    return tag.split("}")[-1] if isinstance(tag, str) else ""

async def walk_sitemap(client, url, out, depth=0):
    if depth > 3 or len(out) >= MAX_URLS_PER_SOURCE:
        return
    root = await get_xml(client, url)
    if root is None:
        return
    if local(root.tag) == "sitemapindex":
        locs = [e.text.strip() for e in root.iter() if local(e.tag) == "loc" and e.text]
        for loc in locs[::-1]:                    # newest sitemaps are usually last
            await walk_sitemap(client, loc, out, depth + 1)
            if len(out) >= MAX_URLS_PER_SOURCE:
                break
    else:
        for e in root.iter():
            if local(e.tag) == "loc" and e.text:
                out.add(e.text.strip())
            elif local(e.tag) == "link" and e.text and e.text.startswith("http"):   # RSS
                out.add(e.text.strip())

SKIP_EXT = (".xml", ".gz", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".pdf",
            ".mp4", ".mp3", ".m4a", ".mov", ".json", ".rss", ".txt")
SKIP_PATHS = ("/video/", "/videos/", "/audio/", "/podcast", "/gallery/", "/galleries/",
              "/photo/", "/photos/", "/live/", "/live-news/", "/tag/", "/tags/", "/topic/",
              "/author/", "/search/", "/sounds/", "/iplayer/")

async def discover_one(pool, client, gate, domain):
    sem = await gate.wait(domain)
    out = set()
    try:
        candidates = []
        try:
            r = await client.get(f"https://{domain}/robots.txt")
            candidates += re.findall(r"(?im)^sitemap:\s*(\S+)", r.text) if r.status_code == 200 else []
        except Exception:
            pass
        candidates += [f"https://{domain}/{p}" for p in
                       ("sitemap.xml", "sitemap_index.xml", "news-sitemap.xml", "sitemap-news.xml",
                        "feed", "rss", "rss.xml", "feed.xml")]
        for c in dict.fromkeys(candidates):
            await walk_sitemap(client, c, out)
            if len(out) >= MAX_URLS_PER_SOURCE:
                break
    except Exception as e:
        print("discover error", domain, e)
    finally:
        sem.release()
    # keep only same-domain, article-looking URLs (has a path beyond '/')
    rows = []
    for u in out:
        p = urlparse(u)
        low = p.path.lower()
        if (p.netloc.endswith(domain) and len(p.path.strip("/")) > 8
                and not low.endswith(SKIP_EXT)
                and not any(s in low for s in SKIP_PATHS)):
            rows.append((u, domain))
    if rows:
        await pool.executemany("INSERT INTO urls(url, domain) VALUES($1,$2) ON CONFLICT DO NOTHING", rows)
    await pool.execute("UPDATE sources SET discovered_at=now() WHERE domain=$1", domain)
    print(f"{domain}: {len(rows)} urls")

async def cmd_discover(pool):
    await pool.execute("ALTER TABLE sources ADD COLUMN IF NOT EXISTS discovered_at TIMESTAMPTZ")
    await pool.execute("UPDATE sources SET discovered_at=now() WHERE discovered_at IS NULL "
                       "AND domain IN (SELECT DISTINCT domain FROM urls)")
    rows = await pool.fetch("SELECT domain FROM sources WHERE status='ok' AND discovered_at IS NULL")
    gate = DomainGate()
    async with make_client(pick_proxy()) as client:
        outer = asyncio.Semaphore(min(CONCURRENCY, 40))
        async def run(d):
            async with outer:
                await discover_one(pool, client, gate, d)
        await asyncio.gather(*(run(r["domain"]) for r in rows))

# ------------------------------------------------------- Tier 2: HTTP fetch
# Takes a few URLs from EACH site per round (round-robin), so all sites run in parallel
# instead of one site's URLs blocking the queue.
CLAIM_SQL = """
WITH picked AS (
    SELECT x.id FROM (
        SELECT domain FROM sources
        WHERE status='ok' AND (cooldown_until IS NULL OR cooldown_until < now())
          AND ((hashtext(domain)::bigint & 2147483647) % $3::int) = $4::int
        ORDER BY random()) s
    CROSS JOIN LATERAL (
        SELECT u.id FROM urls u
        WHERE u.domain = s.domain AND u.status='pending' AND u.next_try_at <= now()
        ORDER BY u.id LIMIT $2 FOR UPDATE SKIP LOCKED) x
    LIMIT $1)
UPDATE urls SET status='in_progress', locked_at=now(), attempts=attempts+1
WHERE id IN (SELECT id FROM picked)
RETURNING id, url, domain, attempts"""

POOL = None   # process pool for CPU-heavy HTML extraction (set in cmd_fetch)
from collections import Counter
STATS = {"n": 0, "gate": 0.0, "http": 0.0, "parse": 0.0, "save": 0.0}
ERRS = Counter()

async def fetch_one(pool, client, gate, row):
    uid, url, domain, attempts = row["id"], row["url"], row["domain"], row["attempts"]
    t0 = time.monotonic()
    sem = await gate.wait(domain)
    released = False
    try:
        t1 = time.monotonic()
        if not await robots_allows(client, domain, url):
            await pool.execute("UPDATE urls SET status='failed', last_error='robots' WHERE id=$1", uid); return
        r = await client.get(url)
        t2 = time.monotonic()
        sem.release(); released = True        # site slot is free as soon as the download is done
        if looks_blocked(r.status_code, r.text):
            ERRS["blocked"] += 1
            # Do NOT try to defeat the challenge: back off this site and retry later.
            mins = min(60, 5 * 2 ** min((await pool.fetchval(
                "SELECT fail_streak FROM sources WHERE domain=$1", domain)) or 0, 4))
            await cooldown(pool, domain, mins)
            await pool.execute("UPDATE sources SET status='blocked', note='blocked on articles' "
                               "WHERE domain=$1 AND fail_streak >= 6", domain)
            await pool.execute("UPDATE urls SET status='pending', locked_at=NULL, last_error=$2, "
                               "next_try_at = now() + make_interval(mins => $3) WHERE id=$1",
                               uid, f"blocked {r.status_code}", mins)
            return
        if r.status_code == 404 or r.status_code == 410:
            await pool.execute("UPDATE urls SET status='failed', last_error='404' WHERE id=$1", uid); return
        r.raise_for_status()
        loop = asyncio.get_running_loop()
        a = await loop.run_in_executor(POOL, extract, r.text, url)
        t3 = time.monotonic()
        if a and a["content"] and not is_english(a["content"]):
            await pool.execute("UPDATE urls SET status='failed', last_error='non-english', locked_at=NULL WHERE id=$1", uid)
            return
        if good(a):
            await save_article(pool, uid, url, domain, a, "http")
            await pool.execute("UPDATE sources SET fail_streak=0 WHERE domain=$1", domain)
            t4 = time.monotonic()
            STATS["n"] += 1; STATS["gate"] += t1 - t0; STATS["http"] += t2 - t1
            STATS["parse"] += t3 - t2; STATS["save"] += t4 - t3
        else:
            await pool.execute("UPDATE urls SET status='needs_js', locked_at=NULL WHERE id=$1", uid)
    except Exception as e:
        ERRS[type(e).__name__] += 1
        status = "failed" if attempts >= 4 else "pending"
        await pool.execute("UPDATE urls SET status=$2, locked_at=NULL, last_error=$3, "
                           "next_try_at = now() + interval '10 minutes' WHERE id=$1",
                           uid, status, str(e)[:200])
    finally:
        if not released:
            sem.release()

async def cmd_fetch(pool):
    # crash recovery: anything left 'in_progress' goes back in the queue
    global POOL
    from concurrent.futures import ProcessPoolExecutor
    shard_i, shard_n = (int(x) for x in os.getenv("FETCH_SHARD", "0/1").split("/"))
    POOL = ProcessPoolExecutor(max_workers=max(2, ((os.cpu_count() or 8) - 2) // shard_n))
    # each fetch process owns a fixed slice of the sites, so a site is only ever scraped by ONE process
    await pool.execute("UPDATE urls SET status='pending', locked_at=NULL WHERE status='in_progress' "
                       "AND ((hashtext(domain)::bigint & 2147483647) % $1::int) = $2::int", shard_n, shard_i)
    await pool.execute("CREATE INDEX IF NOT EXISTS urls_pending_idx ON urls (domain, id) WHERE status='pending'")
    gate = DomainGate()
    async with make_client(pick_proxy()) as client:
        running = set()
        last_log = 0
        while True:
            if time.monotonic() - last_log > 30:
                last_log = time.monotonic()
                d = await pool.fetchval("SELECT count(*) FROM urls WHERE status='done'")
                cool = await pool.fetchval("SELECT count(*) FROM sources WHERE status='ok' AND cooldown_until > now()")
                act = await pool.fetchval("SELECT count(*) FROM sources WHERE status='ok'")
                n = max(STATS["n"], 1)
                print(f"[{time.strftime('%H:%M:%S')}] fetch {shard_i}/{shard_n} in-flight={len(running)} done={d} "
                      f"sites ok={act} cooling={cool} | saved={STATS['n']} avg wait={STATS['gate']/n:.1f}s "
                      f"download={STATS['http']/n:.1f}s parse={STATS['parse']/n:.2f}s save={STATS['save']/n:.2f}s "
                      f"| problems={dict(ERRS.most_common(4))}", flush=True)
                STATS.update(n=0, gate=0.0, http=0.0, parse=0.0, save=0.0); ERRS.clear()
            free = CONCURRENCY - len(running)
            if free > 0:
                for row in await pool.fetch(CLAIM_SQL, free, PER_DOMAIN, shard_n, shard_i):
                    t = asyncio.create_task(fetch_one(pool, client, gate, row))
                    running.add(t); t.add_done_callback(running.discard)
            if not running:
                left = await pool.fetchval("SELECT count(*) FROM urls WHERE status='pending'")
                if not left:
                    break
                await asyncio.sleep(30)        # everything is cooling down
            else:
                await asyncio.sleep(0.2)
    print("fetch finished")

# ---------------------------------------------- Tier 3: browser for JS pages
async def cmd_render(pool):
    from playwright.async_api import async_playwright
    gate = DomainGate()
    todo = await pool.fetchval("SELECT count(*) FROM urls WHERE status='needs_js'")
    print(f"[{time.strftime('%H:%M:%S')}] render: {todo} pages waiting for the browser", flush=True)
    if not todo:
        print("render: nothing to do, will check again soon", flush=True)
        return
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=os.getenv("HEADLESS", "1") != "0")
        async def worker():
            proxy = pick_proxy()
            ctx = await browser.new_context(user_agent=UA, locale="en-US",
                                            proxy={"server": proxy} if proxy else None)
            page = await ctx.new_page()
            while True:
                row = await pool.fetchrow(
                    """UPDATE urls SET status='rendering', locked_at=now(), attempts=attempts+1
                       WHERE id = (SELECT id FROM urls WHERE status='needs_js' LIMIT 1 FOR UPDATE SKIP LOCKED)
                       RETURNING id, url, domain""")
                if not row:
                    return
                sem = await gate.wait(row["domain"])
                try:
                    await page.goto(row["url"], wait_until="domcontentloaded", timeout=40000)
                    await page.mouse.wheel(0, 1500)          # triggers lazy-loaded content
                    await page.wait_for_timeout(1500)
                    html = await page.content()
                    if looks_blocked(200, html):
                        await cooldown(pool, row["domain"], 30)
                        await pool.execute("UPDATE urls SET status='blocked', locked_at=NULL WHERE id=$1", row["id"])
                        continue
                    a = extract(html, row["url"])
                    if good(a):
                        await save_article(pool, row["id"], row["url"], row["domain"], a, "browser")
                        print(f"[{time.strftime('%H:%M:%S')}] render saved: {row['domain']}", flush=True)
                    else:
                        await pool.execute("UPDATE urls SET status='failed', last_error='no article', locked_at=NULL WHERE id=$1", row["id"])
                except Exception as e:
                    await pool.execute("UPDATE urls SET status='failed', last_error=$2, locked_at=NULL WHERE id=$1",
                                       row["id"], str(e)[:200])
                finally:
                    sem.release()
        await pool.execute("UPDATE urls SET status='needs_js' WHERE status='rendering'")
        await asyncio.gather(*(worker() for _ in range(RENDER_WORKERS)))
        await browser.close()
    print(f"[{time.strftime('%H:%M:%S')}] render: finished all waiting pages", flush=True)

# ------------------------------------------------- promote CC-NEWS domains
async def cmd_promote(pool, n=300):
    await pool.execute("CREATE TABLE IF NOT EXISTS cc_domains (domain TEXT PRIMARY KEY, pages INT NOT NULL DEFAULT 0)")
    rows = await pool.fetch("""
        UPDATE sources SET status='new' WHERE domain IN (
            SELECT s.domain FROM sources s
            LEFT JOIN (SELECT domain, count(*) c FROM articles WHERE method='cc-news' GROUP BY domain) a
                   ON a.domain = s.domain
            LEFT JOIN cc_domains d ON d.domain = s.domain
            WHERE s.status='cc'
              AND (COALESCE(a.c,0) >= $2 OR COALESCE(d.pages,0) >= $3)
            ORDER BY GREATEST(COALESCE(a.c,0), COALESCE(d.pages,0)) DESC LIMIT $1)
        RETURNING domain""", n, int(os.getenv("PROMOTE_MIN", "5")), int(os.getenv("PROMOTE_PAGES", "10")))
    print(f"promoted {len(rows)} sources")

# ------------------------------------------------------------------- stats
async def cmd_stats(pool):
    for r in await pool.fetch("SELECT status, count(*) c FROM urls GROUP BY 1 ORDER BY 1"):
        print(f"urls  {r['status']:<12}{r['c']:>12,}")
    for r in await pool.fetch("SELECT status, count(*) c FROM sources GROUP BY 1 ORDER BY 1"):
        print(f"sites {r['status']:<12}{r['c']:>12,}")
    print("articles", f"{await pool.fetchval('SELECT count(*) FROM articles'):,}")

async def main():
    if len(sys.argv) < 2:
        print(__doc__); return
    pool = await asyncpg.create_pool(DSN, min_size=2, max_size=int(os.getenv("DB_POOL", "30")))
    cmd = sys.argv[1]
    try:
        if cmd == "init": await cmd_init(pool)
        elif cmd == "load": await cmd_load(pool, sys.argv[2])
        elif cmd == "check": await cmd_check(pool)
        elif cmd == "discover": await cmd_discover(pool)
        elif cmd == "fetch": await cmd_fetch(pool)
        elif cmd == "render": await cmd_render(pool)
        elif cmd == "promote": await cmd_promote(pool, int(sys.argv[2]) if len(sys.argv) > 2 else 300)
        elif cmd == "stats": await cmd_stats(pool)
        else: print(__doc__)
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())