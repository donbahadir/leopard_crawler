"""
Split scraped markdown JSON into ≤45 MB text chunks
--------------------------------------------------
Given a JSON file like::
    [
      {"url": "https://about.lafayette.edu/foo", "markdown": "# Title\n..."},
      ...
    ]
this script writes one or more plain‑text files (≤45 MB each) under
    /Users/don/Desktop/RAG DATA/final_outputs/
The base filename is the *domain* of the first URL (e.g. `about.lafayette.edu`).
Example output:
    /Users/don/Desktop/RAG DATA/final_outputs/about.lafayette.edu.txt
    /Users/don/Desktop/RAG DATA/final_outputs/about.lafayette.edu_part2.txt
"""

import json
import os
from pathlib import Path
from urllib.parse import urlparse

MAX_SIZE_BYTES = 45 * 1024 * 1024  # 45 MB
OUTPUT_DIR = Path("/Users/don/Desktop/RAG DATA/final_outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DIVIDER = "\n" + "=" * 80 + "\n\n"


def split_json_to_txt(json_file: str) -> None:
    """Write text files for the given scraped‑markdown JSON bundle."""

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        raise ValueError("Input JSON is empty – nothing to write.")

    # Determine domain prefix from first URL
    domain = urlparse(data[0].get("url", "")).netloc or "output"
    prefix = OUTPUT_DIR / domain

    file_count = 1
    current_path = prefix.with_suffix(".txt")
    current_file = current_path.open("w", encoding="utf-8")

    for entry in data:
        url = entry.get("url", "")
        markdown = entry.get("markdown", "")
        if markdown.strip() == "":
            continue

        entry_text = f"URL: {url}\n\n{markdown}{DIVIDER}"
        entry_size = len(entry_text.encode("utf-8"))

        current_file.flush()
        if current_path.stat().st_size + entry_size > MAX_SIZE_BYTES:
            current_file.close()
            file_count += 1
            current_path = OUTPUT_DIR / f"{domain}_part{file_count}.txt"
            current_file = current_path.open("w", encoding="utf-8")

        current_file.write(entry_text)

    current_file.close()
    print(f"✅ Finished writing {file_count} file(s) under {OUTPUT_DIR}/ with base '{domain}'.")


if __name__ == "__main__":
    INPUT_JSON = "/Users/don/Desktop/RAG DATA/scraped_json/academics.lafayette.edu.json"  # ≤ adjust as needed
    split_json_to_txt(INPUT_JSON)
