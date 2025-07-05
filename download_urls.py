import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse, quote, unquote
import os
import time
import re
import mimetypes # Used to guess file extensions from MIME types

def sanitize_filename(url):
    """
    Sanitizes a URL to create a valid and safe filename.
    Replaces problematic characters with underscores and decodes URL parts.
    """
    # Decode URL to handle special characters correctly before sanitizing
    decoded_url = unquote(url)
    
    # Parse the URL to get path and query components
    parsed_url = urlparse(decoded_url)
    path_segments = [s for s in parsed_url.path.split('/') if s]
    
    # Use the last path segment or the netloc if no path
    if path_segments:
        filename = path_segments[-1]
    else:
        filename = parsed_url.netloc
        
    # If there's a query string, append a hash of it to make filenames unique
    if parsed_url.query:
        import hashlib
        query_hash = hashlib.md5(parsed_url.query.encode('utf-8')).hexdigest()[:8]
        if filename:
            filename = f"{filename}_{query_hash}"
        else:
            filename = f"index_{query_hash}" # For root URLs with query
            
    # Remove any file extension from the determined filename for now, we'll add it later
    filename_parts = filename.rsplit('.', 1)
    if len(filename_parts) > 1 and filename_parts[1].isalnum(): # Check if it looks like an extension
        filename = filename_parts[0]

    # Replace invalid characters for filenames with underscores
    filename = re.sub(r'[\\/:*?"<>|]', '_', filename)
    # Trim leading/trailing spaces and dots
    filename = filename.strip(' .')
    # Ensure it's not empty
    if not filename:
        filename = "index"
    return filename

def get_content_type_and_extension(url, response_headers):
    """
    Determines the content type and a suitable file extension for a given URL
    based on response headers and URL path.
    """
    content_type = response_headers.get('Content-Type', '').split(';')[0].strip().lower()
    
    # Try to guess extension from MIME type first
    ext = mimetypes.guess_extension(content_type)
    if ext:
        return content_type, ext

    # If MIME type doesn't give a good extension, try from URL path
    parsed_url = urlparse(url)
    path = parsed_url.path
    
    # Extract extension from path if it exists
    _, url_ext = os.path.splitext(path)
    if url_ext:
        return content_type, url_ext.lower()

    # Fallback for common types if no extension found
    if 'html' in content_type:
        return content_type, '.html'
    elif 'text/' in content_type:
        return content_type, '.txt'
    elif 'json' in content_type:
        return content_type, '.json'
    elif 'xml' in content_type:
        return content_type, '.xml'
    elif 'pdf' in content_type:
        return content_type, '.pdf'
    elif 'image/' in content_type:
        return content_type, '.bin' # Fallback, will be more specific below
    
    return content_type, '.bin' # Default binary if nothing else matches

def download_content_from_urls(url_list_file, output_base_dir="downloaded_content", 
                               politeness_delay=0.5, max_retries=3, retry_delay=1):
    """
    Reads URLs from a text file, downloads their content, and saves them
    into organized folders based on content type. Only downloads content
    if the URL's domain matches 'www.mosdac.gov.in'.

    Args:
        url_list_file (str): Path to the text file containing URLs (one per line).
        output_base_dir (str): Base directory where all downloaded content will be stored.
        politeness_delay (float): Delay in seconds between each URL download request.
        max_retries (int): Maximum number of retries for fetching a URL.
        retry_delay (int): Delay in seconds between retries.
    """
    # Create base output directory if it doesn't exist
    os.makedirs(output_base_dir, exist_ok=True)
    
    # Define subdirectories for different content types
    html_dir = os.path.join(output_base_dir, "html_pages")
    text_dir = os.path.join(output_base_dir, "text_content")
    documents_dir = os.path.join(output_base_dir, "documents")
    images_dir = os.path.join(output_base_dir, "images")
    other_files_dir = os.path.join(output_base_dir, "other_files")

    # Create all subdirectories
    os.makedirs(html_dir, exist_ok=True)
    os.makedirs(text_dir, exist_ok=True)
    os.makedirs(documents_dir, exist_ok=True)
    os.makedirs(images_dir, exist_ok=True)
    os.makedirs(other_files_dir, exist_ok=True)

    urls_to_download = []
    try:
        with open(url_list_file, 'r', encoding='utf-8') as f:
            urls_to_download = [line.strip() for line in f if line.strip()]
        print(f"Found {len(urls_to_download)} URLs in {url_list_file}")
    except FileNotFoundError:
        print(f"Error: URL list file '{url_list_file}' not found.")
        return
    except Exception as e:
        print(f"Error reading URL list file: {e}")
        return

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Define the allowed domain for content scraping
    allowed_domain = "www.mosdac.gov.in"

    for i, url in enumerate(urls_to_download):
        print(f"\nProcessing URL {i+1}/{len(urls_to_download)}: {url}")
        
        parsed_url = urlparse(url)
        if parsed_url.netloc != allowed_domain:
            print(f"Skipping URL: {url} (Domain does not match '{allowed_domain}')")
            continue # Skip to the next URL if domain doesn't match

        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, stream=True, timeout=15)
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

                content_type, file_ext = get_content_type_and_extension(url, response.headers)
                
                # Determine the target directory based on content type
                target_dir = other_files_dir
                if 'text/html' in content_type:
                    target_dir = html_dir
                elif 'application/pdf' in content_type or file_ext == '.pdf':
                    target_dir = documents_dir
                elif 'image/' in content_type:
                    target_dir = images_dir
                elif 'application/msword' in content_type or 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type or file_ext in ['.doc', '.docx']:
                    target_dir = documents_dir
                elif 'application/vnd.ms-excel' in content_type or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or file_ext in ['.xls', '.xlsx']:
                    target_dir = documents_dir
                elif 'application/vnd.ms-powerpoint' in content_type or 'application/vnd.openxmlformats-officedocument.presentationml.presentation' in content_type or file_ext in ['.ppt', '.pptx']:
                    target_dir = documents_dir
                elif 'application/zip' in content_type or file_ext == '.zip':
                    target_dir = other_files_dir # Or create a 'archives' folder if needed
                
                base_filename = sanitize_filename(url)
                
                # Handle HTML content specially: save raw HTML and extracted text
                if target_dir == html_dir:
                    html_filepath = os.path.join(target_dir, f"{base_filename}.html")
                    text_filepath = os.path.join(text_dir, f"{base_filename}.txt")
                    
                    # Save raw HTML
                    with open(html_filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"Saved HTML to: {html_filepath}")

                    # Extract and save plain text
                    try:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        for script_or_style in soup(['script', 'style']):
                            script_or_style.decompose()
                        text_content = soup.get_text(separator='\n', strip=True)
                        text_content = re.sub(r'\n\s*\n', '\n\n', text_content)
                        text_content = re.sub(r'[ \t]+', ' ', text_content)
                        
                        with open(text_filepath, 'w', encoding='utf-8') as f:
                            f.write(text_content)
                        print(f"Saved plain text to: {text_filepath}")
                    except Exception as e:
                        print(f"Error extracting text from HTML for {url}: {e}")
                else:
                    # For other file types, save directly
                    filepath = os.path.join(target_dir, f"{base_filename}{file_ext}")
                    # Ensure unique filename if it already exists
                    counter = 1
                    original_filepath = filepath
                    while os.path.exists(filepath):
                        filepath = os.path.join(target_dir, f"{base_filename}_{counter}{file_ext}")
                        counter += 1

                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"Saved {content_type} to: {filepath}")
                
                break # Break from retry loop if successful

            except requests.exceptions.Timeout:
                print(f"Timeout occurred while fetching {url}. Retrying ({attempt + 1}/{max_retries})...")
            except requests.exceptions.RequestException as e:
                print(f"Error fetching {url}: {e}. Retrying ({attempt + 1}/{max_retries})...")
            except IOError as e:
                print(f"Error saving file for {url}: {e}. Retrying ({attempt + 1}/{max_retries})...")
            except Exception as e:
                print(f"An unexpected error occurred for {url}: {e}. Retrying ({attempt + 1}/{max_retries})...")
            
            if attempt < max_retries - 1:
                time.sleep(retry_delay) # Wait before retrying
        else:
            print(f"Failed to download {url} after {max_retries} attempts.")

        time.sleep(politeness_delay) # Politeness delay between URLs

# --- Example Usage ---
if __name__ == "__main__":
    # Ensure you have a 'mosdac_urls.txt' file generated by the previous script
    # or create one manually with URLs, one per line.
    url_list_file = "mosdac_urls.txt" 
    output_directory = "mosdac_downloaded_content"

    print(f"Starting content download from URLs in '{url_list_file}' into '{output_directory}'...")
    download_content_from_urls(url_list_file, output_directory, 
                               politeness_delay=0.5, max_retries=3, retry_delay=2)
    print("\nContent download and organization complete!")
    print(f"Check the '{output_directory}' folder for the downloaded files.")
