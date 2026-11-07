"""
Load news articles from Common Crawl's CC-NEWS dataset into the same Postgres tables.
No news website is contacted: pages are read from Common Crawl's public archive files.

  pip install warcio
  python cc_news.py 2026 09                 # all files for Sept 2026
  python cc_news.py 2026 09 --max 5         # test with 5 files first
  python cc_news.py 2026 09 --shard 0/4     # run 4 copies (0/4, 1/4, 2/4, 3/4) in 4 terminals
  python cc_news.py 2026 09 --domains       # DOMAINS ONLY: no parsing, just records which sites appear
                                            # (fast way to find thousands of new sources)

Resumable: finished files are recorded in table cc_files. Re-run the same command to continue.
Needs DB_DSN set (same as scraper.py) and scraper.py in the same folder.
"""
import asyncio, gzip, os, re, sys, urllib.request
from collections import Counter
from urllib.parse import urlparse

import asyncpg
from dateutil import parser as dtparse
from warcio.archiveiterator import ArchiveIterator

from scraper import extract, good, is_english, DSN, TZINFOS

BASE = "https://data.commoncrawl.org/"
# CC-NEWS is worldwide. Keep mostly US-style domains; add or remove TLDs as you like.
OK_TLDS = {"com", "org", "net", "us", "news"}
MAX_BODY = 3_000_000
BATCH = 200


def open_url(path):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": "academic-news-research"})
    return urllib.request.urlopen(req, timeout=180)


def clean(s):
    return s.replace("\x00", "") if isinstance(s, str) else s


async def flush(pool, rows):
    if not rows:
        return
    domains = {(r[1],) for r in rows}
    async with pool.acquire() as c, c.transaction():
        await c.executemany("INSERT INTO sources(domain, status) VALUES($1,'cc') ON CONFLICT DO NOTHING", list(domains))
        await c.executemany(
            """INSERT INTO articles (url, domain, headline, content, author, published_at, method)
               VALUES ($1,$2,$3,$4,$5,$6,'cc-news') ON CONFLICT (url) DO NOTHING""", rows)
    rows.clear()


async def process_file(pool, path):
    rows, seen, saved = [], 0, 0
    with open_url(path) as stream:
        for rec in ArchiveIterator(stream):
            if rec.rec_type != "response" or rec.http_headers is None:
                continue
            if not rec.http_headers.get_statuscode().startswith("200"):
                continue
            if "html" not in (rec.http_headers.get_header("Content-Type") or "").lower():
                continue
            url = rec.rec_headers.get_header("WARC-Target-URI")
            host = urlparse(url).netloc.lower().removeprefix("www.")
            if host.rsplit(".", 1)[-1] not in OK_TLDS or len(urlparse(url).path.strip("/")) < 9:
                continue
            body = rec.content_stream().read()
            if len(body) > MAX_BODY:
                continue
            seen += 1
            html = body.decode("utf-8", "replace")
            try:
                a = extract(html, url, fast=True)
            except Exception:
                continue           # skip one bad page, keep the rest of the file
            if not good(a) or not is_english(a["content"]):
                continue
            if a["published_at"] is None:
                try:
                    a["published_at"] = dtparse.parse(rec.rec_headers.get_header("WARC-Date"), tzinfos=TZINFOS)
                except Exception:
                    pass
            rows.append((clean(url), host, clean(a["headline"]), clean(a["content"]),
                         clean(a["author"]), a["published_at"]))
            saved += 1
            if len(rows) >= BATCH:
                await flush(pool, rows)
                await asyncio.sleep(0)
    await flush(pool, rows)
    return seen, saved


LANG_RE = re.compile(rb"<html[^>]{0,300}?\blang=[\"']?([A-Za-z-]+)", re.I)


async def process_file_domains(pool, path):
    """Read one archive file WITHOUT parsing articles; count pages per domain."""
    counts, pages = Counter(), 0
    with open_url(path) as stream:
        for rec in ArchiveIterator(stream):
            if rec.rec_type != "response" or rec.http_headers is None:
                continue
            if not rec.http_headers.get_statuscode().startswith("200"):
                continue
            if "html" not in (rec.http_headers.get_header("Content-Type") or "").lower():
                continue
            url = rec.rec_headers.get_header("WARC-Target-URI")
            host = urlparse(url).netloc.lower().removeprefix("www.")
            if host.rsplit(".", 1)[-1] not in OK_TLDS or len(urlparse(url).path.strip("/")) < 9:
                continue
            head = rec.content_stream().read(4096)
            m = LANG_RE.search(head)
            if m and not m.group(1).lower().startswith(b"en"):
                continue                      # page says it is not English
            pages += 1
            counts[host] += 1
    async with pool.acquire() as c, c.transaction():
        await c.execute("CREATE TABLE IF NOT EXISTS cc_domains (domain TEXT PRIMARY KEY, pages INT NOT NULL DEFAULT 0)")
        await c.executemany(
            "INSERT INTO cc_domains(domain, pages) VALUES($1,$2) "
            "ON CONFLICT (domain) DO UPDATE SET pages = cc_domains.pages + EXCLUDED.pages",
            list(counts.items()))
        await c.executemany("INSERT INTO sources(domain, status) VALUES($1,'cc') ON CONFLICT DO NOTHING",
                            [(d,) for d in counts])
    return pages, len(counts)


async def main():
    args = sys.argv[1:]
    if len(args) < 2:
        print(__doc__); return
    year, month = args[0], args[1].zfill(2)
    max_files = int(args[args.index("--max") + 1]) if "--max" in args else None
    shard_i, shard_n = (0, 1)
    if "--shard" in args:
        shard_i, shard_n = map(int, args[args.index("--shard") + 1].split("/"))

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    await pool.execute("CREATE TABLE IF NOT EXISTS cc_files (path TEXT PRIMARY KEY, seen INT, saved INT, done_at TIMESTAMPTZ DEFAULT now())")
    with open_url(f"crawl-data/CC-NEWS/{year}/{month}/warc.paths.gz") as f:
        paths = gzip.decompress(f.read()).decode().split()
    print(f"{len(paths)} files listed for {year}-{month}")
    done = {r["path"] for r in await pool.fetch("SELECT path FROM cc_files")}
    domains_mode = "--domains" in args

    n = 0
    for idx, path in enumerate(paths):
        key = ("dom:" + path) if domains_mode else path
        if idx % shard_n != shard_i or key in done:
            continue
        if max_files is not None and n >= max_files:
            break
        n += 1
        try:
            if domains_mode:
                seen, found = await process_file_domains(pool, path)
                await pool.execute("INSERT INTO cc_files(path, seen, saved) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", key, seen, found)
                print(f"done (domains) {path}  pages={seen}  domains_found={found}", flush=True)
                continue
            seen, saved = await process_file(pool, path)
            await pool.execute("INSERT INTO cc_files(path, seen, saved) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", path, seen, saved)
            print(f"done {path}  pages={seen}  saved={saved}", flush=True)
        except Exception as e:
            print(f"ERROR {path}: {e} (will retry next run)", flush=True)
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())