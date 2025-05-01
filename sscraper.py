"""
Scrape pages to markdown and save per‑subdomain JSON
---------------------------------------------------
Given a JSON file produced by the crawler (html/document URLs), fetch each
URL, generate cleaned markdown (headers/navs removed), and store the result
in `/Users/don/Desktop/RAG DATA/scraped_json/<subdomain>.json`.

Example output path for about.lafayette.edu →
    /Users/don/Desktop/RAG DATA/scraped_json/about.lafayette.edu.json
"""

import asyncio
import json
import os
import re
from urllib.parse import urlparse, urlunparse, urljoin

from crawl4ai import (
    AsyncWebCrawler,
    MemoryAdaptiveDispatcher,
    CrawlerMonitor,
    RateLimiter,
)
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

# ---------------------------------------------------------------------------
# 🔧  Configuration
# ---------------------------------------------------------------------------

# Input file containing URLs (produced by the crawler)
INPUT_FILE = (
    "/Users/don/Desktop/RAG DATA/crawled_links/academics.lafayette.edu_crawled_links.json"  # ⬅ adjust as needed or pass via CLI
)

# Output directory for scraped markdown JSON files
OUTPUT_DIR = "/Users/don/Desktop/RAG DATA/scraped_json"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Optional pruning filter — currently disabled
prune_filter = PruningContentFilter(
    threshold_type="fixed",
    threshold=0.5,
    min_word_threshold=30,
)

# JS snippet to strip common chrome (header/nav/footer/buttons)
JS_REMOVE_ELEMENTS = """
(async () => {
  const selectors = [
    'header', 'nav', '.menu', '.navbar', '.site-header', '.global-nav',
    'footer', '.footer', '.site-footer', '#footer',
    'a.btn',                       // e.g. Map Search button
    'a[href*="FuseAction=Programs."]'
  ];
  selectors.forEach(sel => {
    document.querySelectorAll(sel).forEach(el => el.remove());
  });
})();
"""

# ---------------------------------------------------------------------------
# 🕸️  Crawl batch
# ---------------------------------------------------------------------------

async def crawl_batch(urls: list[str]) -> list[dict[str, str]]:
    """Fetch each URL and return list of {url, markdown}."""

    browser_config = BrowserConfig(headless=True, verbose=False)

    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": True,
            "ignore_images": True,
            "skip_internal_links": True,
        },
        # content_filter=prune_filter,
    )

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        stream=False,
        check_robots_txt=True,
        markdown_generator=md_generator,
        js_code=[JS_REMOVE_ELEMENTS],
    )

    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=90.0,
        check_interval=3.0,
        max_session_permit=5,
        rate_limiter=RateLimiter(base_delay=(1.0, 2.0), max_delay=60.0, max_retries=2),
        monitor=CrawlerMonitor(),
    )

    output: list[dict[str, str]] = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        results = await crawler.arun_many(urls, config=run_config, dispatcher=dispatcher)

        for res in results:
            if res.success and res.markdown.raw_markdown:
                output.append({"url": res.url, "markdown": res.markdown.raw_markdown})
            else:
                print(f"⚠️  Failed or empty markdown for {res.url}: {res.error_message}")

    return output

# ---------------------------------------------------------------------------
# 🚀  Main
# ---------------------------------------------------------------------------

async def main() -> None:
    # ------------ Load URL list -------------
    with open(INPUT_FILE, "r", encoding="utf-8") as fp:
        data = json.load(fp)

    # Support both {"html": [...]} and plain list formats
    if isinstance(data, dict):
        urls = data.get("html", []) + data.get("document", [])
    else:
        urls = data

    if not urls:
        raise ValueError("No URLs found in input file.")

    # Determine subdomain for naming output file
    domain = urlparse(urls[0]).netloc  # about.lafayette.edu, etc.

    scraped = await crawl_batch(urls)

    out_path = os.path.join(OUTPUT_DIR, f"{domain}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(scraped, f, indent=2, ensure_ascii=False)

    print("=" * 60)
    print(f"Saved {len(scraped)} pages → {out_path}")

if __name__ == "__main__":
    asyncio.run(main())
