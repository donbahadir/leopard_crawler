import asyncio
import json
import re
import os
from collections import deque
from urllib.parse import urlparse, urlunparse
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode

"""
Exhaustive sub‑domain crawler
-----------------------------
• Traverses every internal link starting from each seed URL (no depth cap).
• Skips any URL whose fragment is exactly "#page".
• Persists **one flat list** of links (HTML + docs together) per seed, i.e. the
  JSON file is simply `[
      "https://academics.lafayette.edu/",
      "https://academics.lafayette.edu/xyz.pdf",
      ...
  ]`
  — no separate "html" / "document" keys.
"""

# Path to JSON file with seed URLs
JSON_PATH = "/Users/don/Desktop/RAG DATA/alturls.json"

# ---------------------------------------------------------------------------
# 🗂️  Load / initialise START_URLS
# ---------------------------------------------------------------------------
if not os.path.exists(JSON_PATH):
    START_URLS: list[str] = []
    with open(JSON_PATH, "w", encoding="utf-8") as fp:
        json.dump(START_URLS, fp, indent=2)
    print(f"Created new JSON file at {JSON_PATH} with an empty list.")
else:
    with open(JSON_PATH, "r", encoding="utf-8") as fp:
        try:
            START_URLS: list[str] = json.load(fp)
        except json.decoder.JSONDecodeError:
            START_URLS = []
            print("JSON file was empty or malformed – initialised with an empty list.")

if not START_URLS:
    default_url = "https://academics.lafayette.edu/"
    START_URLS = [default_url]
    with open(JSON_PATH, "w", encoding="utf-8") as fp:
        json.dump(START_URLS, fp, indent=2)
    print(f"No URLs found in JSON → added default URL: {default_url}")

BATCH_SIZE = 8  # concurrent crawls

# ---------------------------------------------------------------------------
# 🌐  AsyncWebCrawler configuration
# ---------------------------------------------------------------------------
BROWSER_CONFIG = BrowserConfig(headless=True, verbose=True)
RUN_CONFIG = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,
    word_count_threshold=10,
    remove_overlay_elements=True,
    check_robots_txt=True,
    user_agent_mode="random",
)

# ---------------------------------------------------------------------------
# 🔗  Helper utilities
# ---------------------------------------------------------------------------

def normalize_url(url: str) -> str:
    """Force https & strip leading www. (except for ee.hacettepe.edu.tr)."""
    parsed = urlparse(url)
    scheme = "https"
    netloc = parsed.netloc
    if not netloc.endswith("ee.hacettepe.edu.tr") and netloc.startswith("www."):
        netloc = netloc[4:]
    return urlunparse((scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))


def is_html_or_doc(url: str) -> bool:
    path = urlparse(url).path
    if path.endswith("/") or "." not in path.split("/")[-1]:
        return True
    return path.lower().endswith((
        ".html", ".htm", ".php", ".pdf", ".doc", ".docx", ".xls", ".xlsx"
    ))


def is_excluded(url: str) -> bool:
    exclude_patterns: list[str] = [
        # add custom regexes to skip undesired URLs
    ]
    return any(re.search(pat, url) for pat in exclude_patterns)


def has_page_fragment(url: str) -> bool:
    return urlparse(url).fragment.lower() == "page"

# ---------------------------------------------------------------------------
# 🕸️  Crawler core
# ---------------------------------------------------------------------------

async def crawl_site(seed: str) -> None:
    """Crawl one seed URL and dump a single flat list of discovered links."""

    visited: set[str] = set()
    queued: set[str]  = set()
    out_links: list[str] = []

    seed_norm = normalize_url(seed)
    domain = urlparse(seed_norm).netloc

    queue: deque[tuple[str, int]] = deque([(seed_norm, 0)])
    queued.add(seed_norm)

    async with AsyncWebCrawler(config=BROWSER_CONFIG) as crawler:
        while queue:
            url, depth = queue.popleft()
            if url in visited or has_page_fragment(url):
                continue

            result = await crawler.arun(url, config=RUN_CONFIG)
            if not result.success:
                print(f"⚠️  Error crawling {url}: {result.error_message}")
                continue
            if ("the requested url" in result.markdown.lower()
                and "was not found on this server." in result.markdown.lower()):
                print(f"🚫 Skipping 404 page {url}")
                continue

            visited.add(url)
            out_links.append(url)

            for link in result.links.get("internal", []):
                raw = link.get("href")
                if not raw:
                    continue
                child = normalize_url(raw)
                if has_page_fragment(child):
                    continue
                child_domain = urlparse(child).netloc
                if not (child_domain == domain or child_domain.endswith("." + domain)):
                    continue
                if child in visited or child in queued:
                    continue
                if is_excluded(child) or not is_html_or_doc(child):
                    continue
                queue.append((child, depth + 1))
                queued.add(child)

    # -------------------------------------------------------------------
    # 💾  Save results (flat list)
    # -------------------------------------------------------------------
    out_dir = "/Users/don/Desktop/RAG DATA/crawled_links"
    os.makedirs(out_dir, exist_ok=True)
    outfile = os.path.join(out_dir, f"{domain.replace('/', '_')}_crawled_links.json")

    # Ensure seed is first
    if seed_norm not in out_links:
        out_links.insert(0, seed_norm)

    with open(outfile, "w", encoding="utf-8") as fp:
        json.dump(out_links, fp, indent=2, ensure_ascii=False)

    print("=" * 60)
    print(f"Saved {len(out_links)} links → {outfile}")

# ---------------------------------------------------------------------------
# 🚀  Batch runner
# ---------------------------------------------------------------------------

async def run_batches() -> None:
    for idx in range(0, len(START_URLS), BATCH_SIZE):
        batch = START_URLS[idx : idx + BATCH_SIZE]
        await asyncio.gather(*(crawl_site(url) for url in batch))
        print("=" * 60)
        print(f"Batch {idx // BATCH_SIZE + 1} of {((len(START_URLS)-1)//BATCH_SIZE)+1} completed\n")
        await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(run_batches())
