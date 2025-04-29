import json
import os

def split_json_to_txt(json_file, output_prefix, max_size_bytes=45 * 1024 * 1024, folder_name="cs_outputs"):
    """
    Reads a JSON file with entries having 'url' and 'markdown' keys,
    and writes them to text files, ensuring that no file exceeds max_size_bytes.
    
    Parameters:
        json_file (str): Path to the input JSON file.
        output_prefix (str): Prefix for the output text files.
        max_size_bytes (int): Maximum file size in bytes (default is 45 MB).
    """
    # Read the JSON data
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    file_count = 1
    current_file_name = f"{folder_name}/{output_prefix}_part{file_count}.txt"
    current_file = open(current_file_name, 'w', encoding='utf-8')
    
    # Divider to separate each entry in the output file
    divider = "\n" + "=" * 80 + "\n\n"
    
    for entry in data:
        url = entry.get('url', '')
        markdown = entry.get('markdown', '')
        if markdown == "\n":
            continue
        entry_text = f"URL: {url}\n\n{markdown}\n\n"
        
        # Determine the size in bytes of the new entry
        entry_bytes = len(entry_text.encode('utf-8'))
        current_file.flush()
        current_size = os.path.getsize(current_file_name)
        
        # If writing this entry would exceed our max file size, close the current file and open a new one.
        if current_size + entry_bytes > max_size_bytes:
            current_file.close()
            file_count += 1
            current_file_name = f"{folder_name}/{output_prefix}_part{file_count}.txt"
            current_file = open(current_file_name, 'w', encoding='utf-8')
        
        current_file.write(entry_text)
    
    current_file.close()

    # If only one file was created, rename it to output_prefix.txt
    if file_count == 1:
        new_name = f"{folder_name}/{output_prefix}.txt"
        os.rename(f"{folder_name}/{output_prefix}_part1.txt", new_name)
        print(f"Finished writing 1 file: {new_name}")
    else:
        print(f"Finished writing {file_count} files.")

filename = "terradottafinalclean"
cs_test = "../batch_scraper/cs_jsons/{filename}.json"



split_json_to_txt(f"/Users/don/Desktop/RAG DATA/final_output.json", filename, folder_name="final_outputs")