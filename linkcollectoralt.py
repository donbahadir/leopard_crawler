import asyncio
import json
import re
import os
from collections import deque
from urllib.parse import urlparse, urlunparse
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode

# Set the path for your JSON file
json_path = "/Users/don/Desktop/RAG DATA/alturls.json"

# Check if the file exists; if not, create it with an empty list.
if not os.path.exists(json_path):
    START_URLS = []  # start with an empty list
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(START_URLS, f, indent=2)
    print(f"Created new JSON file at {json_path} with an empty list.")
else:
    with open(json_path, "r", encoding="utf-8") as f:
        try:
            START_URLS = json.load(f)
        except json.decoder.JSONDecodeError:
            START_URLS = []
            print("JSON file was empty or malformed. Initialized with an empty list.")

# If START_URLS is empty, add a default URL (or prompt the user to update the file)
if not START_URLS:
    default_url = "https://lafayette-sa.terradotta.com/index.cfm?FuseAction=Programs.ListAll"
    START_URLS.append(default_url)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(START_URLS, f, indent=2)
    print(f"No URLs found in JSON. Added default URL: {default_url}")

BATCH_SIZE = 8    # Number of concurrent crawls per batch
MAX_DEPTH = 5     # Limit recursion depth

browser_config = BrowserConfig(headless=True, verbose=True)
run_config = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,
    word_count_threshold=10,
    remove_overlay_elements=True,
    check_robots_txt=True,
    user_agent_mode="random",
)

def normalize_url(url):
    """Force URL to HTTPS and remove 'www.' prefix unless the domain is ee.hacettepe.edu.tr."""
    parsed = urlparse(url)
    scheme = "https"
    netloc = parsed.netloc
    if not netloc.endswith("ee.hacettepe.edu.tr") and netloc.startswith("www."):
        netloc = netloc[4:]
    return urlunparse((scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))

def is_html_or_pdf(url):
    parsed = urlparse(url)
    path = parsed.path
    if path.endswith("/") or '.' not in path.split("/")[-1]:
        return True
    return path.lower().endswith((".html", ".pdf", ".php", ".doc", ".docx", ".xls", ".xlsx", ".htm"))

def is_excluded(url):
    exclude_patterns = [
        # Add any regex patterns here to exclude URLs if needed
    ]
    return any(re.match(pattern, url) for pattern in exclude_patterns)

async def get_links(start_url):
    visited_pages = set()
    collected_urls = set()
    start_url_norm = normalize_url(start_url)
    urls_to_crawl = deque([(start_url_norm, 0)])  # (URL, depth)
    html_links, document_links = [], []

    start_domain = urlparse(start_url_norm).netloc

    async with AsyncWebCrawler(config=browser_config) as crawler:
        while urls_to_crawl:
            current_url, depth = urls_to_crawl.popleft()
            if depth >= MAX_DEPTH or current_url in visited_pages:
                continue

            result = await crawler.arun(current_url, config=run_config)
            if not result.success:
                print(f"Error crawling {current_url}: {result.error_message}")
                continue
            if ("the requested url" in result.markdown.lower() and 
                "was not found on this server." in result.markdown.lower()):
                print(f"Skipping page: {current_url} - Not Found")
                continue

            visited_pages.add(current_url)
            for link in result.links.get("internal", []):
                raw_url = link.get("href")
                if not raw_url:
                    continue
                url = normalize_url(raw_url)
                link_domain = urlparse(url).netloc
                if not (link_domain == start_domain or link_domain.endswith("." + start_domain)):
                    continue
                if url in visited_pages or url in collected_urls:
                    continue
                if is_excluded(url) or not is_html_or_pdf(url):
                    continue

                if url.lower().endswith((".pdf", ".doc", ".docx", ".xls", ".xlsx")):
                    document_links.append(url)
                    collected_urls.add(url)
                else:
                    html_links.append(url)
                    urls_to_crawl.append((url, depth + 1))
                    collected_urls.add(url)

        # ------------------------------------------------------------
    # ✏️  SAVE RESULTS
    # ------------------------------------------------------------
    output_dir = "/Users/don/Desktop/RAG DATA/crawled_links"  # <-- NEW LOCATION
    os.makedirs(output_dir, exist_ok=True)

    file_name = (
        f"{output_dir}/"
        f"{start_url_norm.replace('https://', '').replace('/', '_')}_crawled_links.json"
    )

    if start_url_norm not in html_links:
        html_links.append(start_url_norm)

    with open(file_name, "w", encoding="utf-8") as file:
        json.dump({"html": html_links, "document": document_links},
                  file, indent=2, ensure_ascii=False)

    print("=" * 50)
    print(f"Saved results for {start_url_norm} to {file_name}.")
    print(
        f"Collected {len(html_links)} HTML links and "
        f"{len(document_links)} Document links from {start_url_norm}."
    )

async def run_in_batches():
    for i in range(0, len(START_URLS), BATCH_SIZE):
        batch = START_URLS[i:i + BATCH_SIZE]
        await asyncio.gather(*(get_links(url) for url in batch))
        print("="*50)
        print(f"Batch {i // BATCH_SIZE + 1} completed.")
        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run_in_batches())
