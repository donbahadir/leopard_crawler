import asyncio
import json
import re
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai import AsyncWebCrawler, MemoryAdaptiveDispatcher, CrawlerMonitor, RateLimiter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

# Optionally, define a content filter if desired.
prune_filter = PruningContentFilter(
    threshold_type="fixed",  # or "dynamic"
    threshold=0.5,
    min_word_threshold=30,
)

def clean_markdown(markdown_text):
    """
    Removes unwanted lines from the markdown output.
    Adjust unwanted_phrases list to include any text you wish to remove.
    """
    unwanted_phrases = [
        "Skip to content",
        "Loading...",
        "Announcements",
        "List All",
        "Advanced Search",
        "Featured Programs",
        "Map Search",
        "Program Discovery"
    ]
    filtered_lines = []
    for line in markdown_text.splitlines():
        # If any unwanted phrase is in the line, skip that line
        if any(phrase in line for phrase in unwanted_phrases):
            continue
        filtered_lines.append(line)
    return "\n".join(filtered_lines)

async def crawl_batch(urls):
    output = []
    
    # Browser configuration: run headless
    browser_config = BrowserConfig(headless=True, verbose=False)
    
    # Markdown generator configuration. Uncomment content_filter if needed.
    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": True,
            "ignore_images": True,
            "skip_internal_links": True
        },
        # content_filter=prune_filter,
    )

    # JS snippet to remove common unwanted structural elements (e.g., headers, navs, footers)
    js_remove_elements = """
    (async () => {
    const selectors = [
        'header', 
        'nav', 
        '.menu', 
        '.navbar', 
        '.site-header', 
        '.global-nav',
        'footer', 
        '.footer', 
        '.site-footer', 
        '#footer',
        // Add the announcements container selector below:
        '.announcements-box'
    ];
    selectors.forEach(selector => {
        document.querySelectorAll(selector).forEach(el => el.remove());
    });
})();
    """

    # Crawl run configuration.
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        stream=False,  # Collect all results at once.
        check_robots_txt=True,
        markdown_generator=md_generator,
        js_code=[js_remove_elements]
    )

    # Dispatcher configuration:
    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=90.0,
        check_interval=3.0,
        max_session_permit=5,
        rate_limiter=RateLimiter(
            base_delay=(1.0, 2.0),
            max_delay=60.0,
            max_retries=2
        ),
        monitor=CrawlerMonitor()
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        results = await crawler.arun_many(
            urls=urls,
            config=run_config,
            dispatcher=dispatcher
        )

        # Process each crawl result.
        for result in results:
            if result.success:
                print("=" * 50)
                print(f"URL: {result.url}")
                print("Raw Markdown length:", len(result.markdown.raw_markdown))
                print("Fit Markdown length:", len(result.markdown.fit_markdown))
                if len(result.markdown.raw_markdown) > 0:
                    # Clean the markdown output using our function.
                    cleaned = clean_markdown(result.markdown.raw_markdown)
                    output.append({
                        "url": result.url,
                        "markdown": cleaned,
                    })
                else:
                    print(f"Empty markdown for {result.url}")
            else:
                print(f"Failed to crawl {result.url}: {result.error_message}")

    return output

async def main():
    # Update this path to your JSON file containing URLs.
    path = "/Users/don/Desktop/RAG DATA/crawled_links/lafayette-sa.terradotta.com_index.cfm?FuseAction=Programs.ListAll_crawled_links.json"
    with open(path, "r") as f:
        urls = json.load(f)

    output = await crawl_batch(urls)

    # Write the collected output to a JSON file.
    with open("final_output.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    asyncio.run(main())
