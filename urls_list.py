import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import re
import os

# Global sets and lists for tracking during a single scrape operation.
# These will be cleared or re-initialized by the main function for each new scrape call.
_visited_urls = set() # URLs for which content has been scraped in the current run
_all_discovered_urls = set() # All unique URLs found in the current run
_all_extracted_content = [] # Content of pages scraped in the current run

# List of common file extensions to identify non-HTML pages.
# URLs ending with these extensions will be added to _all_discovered_urls but
# their content will not be scraped recursively for more links.
_FILE_EXTENSIONS = {
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.rar',
    '.tar', '.gz', '.bz2', '.7z', '.exe', '.dmg', '.apk',
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
    '.mp3', '.wav', '.ogg', '.flac',
    '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm',
    '.txt', '.csv', '.json', '.xml', '.yaml', '.md',
    '.css', '.js', '.php', '.asp', '.aspx', '.jsp', '.cfm', '.cgi'
}

def _is_file_url(url):
    """
    Checks if a given URL points to a common file type based on its extension.
    This is a helper function, intended for internal use within the scraping module.
    """
    parsed_url = urlparse(url)
    path = parsed_url.path.lower()
    for ext in _FILE_EXTENSIONS:
        if path.endswith(ext):
            return True
    return False

def _get_page_content(url, max_retries=3, delay=1):
    """
    Fetches the HTML content of a given URL, parses it using BeautifulSoup,
    and extracts all visible text and all unique absolute links found within the page.
    Includes retry logic for transient network issues.

    Args:
        url (str): The URL of the page to fetch.
        max_retries (int): Maximum number of retries for fetching the page.
        delay (int): Delay in seconds between retries.

    Returns:
        tuple: A tuple containing:
            - str: The extracted text content of the page (cleaned).
            - list: A list of all unique absolute URLs found on the page (from 'href' attributes).
            Returns (None, None) if an error persists after retries.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            print(f"Fetching content from: {url} (Attempt {attempt + 1}/{max_retries})")
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

            soup = BeautifulSoup(response.text, 'html.parser')

            # --- Text Extraction ---
            for script_or_style in soup(['script', 'style']):
                script_or_style.decompose()
            text_content = soup.get_text(separator='\n', strip=True)
            text_content = re.sub(r'\n\s*\n', '\n\n', text_content)
            text_content = re.sub(r'[ \t]+', ' ', text_content)

            # --- Link Extraction ---
            found_links = set()
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(url, href)
                parsed_full_url = urlparse(full_url)

                if parsed_full_url.scheme in ['http', 'https'] and \
                   not parsed_full_url.fragment and \
                   not full_url.startswith('mailto:'):
                    normalized_url = parsed_full_url._replace(fragment="").geturl()
                    found_links.add(normalized_url)
            
            return text_content, list(found_links)

        except requests.exceptions.Timeout:
            print(f"Timeout occurred while fetching {url}. Retrying...")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}. Retrying...")
        except Exception as e:
            print(f"An unexpected error occurred for {url}: {e}. Retrying...")
        
        if attempt < max_retries - 1:
            time.sleep(delay) # Wait before retrying
    
    print(f"Failed to fetch {url} after {max_retries} attempts.")
    return None, None # Return None for content and links after all retries fail

def _recursive_scrape(base_url_netloc, current_url, max_depth, current_depth, 
                      scrape_content_limit, politeness_delay):
    """
    Internal recursive function to perform the actual web scraping.
    It fetches content from pages that are internal, not identified as files,
    and within the specified depth and page limits. It also discovers and records
    all unique URLs encountered.
    """
    global _all_discovered_urls, _visited_urls, _all_extracted_content

    _all_discovered_urls.add(current_url)

    should_scrape_content = (
        current_url not in _visited_urls and
        current_depth <= max_depth and
        len(_all_extracted_content) < scrape_content_limit
    )

    text_content = None
    found_links = None

    if should_scrape_content:
        _visited_urls.add(current_url)
        text_content, found_links = _get_page_content(current_url)

        if text_content:
            _all_extracted_content.append({'url': current_url, 'text': text_content})
    else:
        # Even if not scraping content due to limits, if this is a new URL
        # not yet processed for its links, fetch its links.
        if current_url not in _visited_urls:
            _visited_urls.add(current_url) # Mark as visited to avoid re-fetching
            _, found_links = _get_page_content(current_url) # Only interested in links here

    if found_links:
        for link in found_links:
            _all_discovered_urls.add(link)

            parsed_link = urlparse(link)
            is_internal = parsed_link.netloc == base_url_netloc
            is_file = _is_file_url(link)

            if is_internal and not is_file and link not in _visited_urls and current_depth < max_depth:
                if len(_all_extracted_content) >= scrape_content_limit:
                    print(f"Reached maximum content page limit ({scrape_content_limit}). Stopping further content scraping recursion.")
                    break
                
                time.sleep(politeness_delay)
                _recursive_scrape(base_url_netloc, link, max_depth, current_depth + 1, 
                                  scrape_content_limit, politeness_delay)

def scrape_website_to_file(start_url, output_filename="discovered_urls.txt", 
                           max_pages_to_visit=50, max_depth=3, politeness_delay=0.5):
    """
    Scrapes a website, extracts content from internal pages, discovers all unique URLs,
    and saves the list of discovered URLs to a text file.

    Args:
        start_url (str): The initial URL from which to start scraping.
        output_filename (str): The name of the text file to save the discovered URLs.
        max_pages_to_visit (int): Maximum number of unique pages for which content
                                  will be extracted.
        max_depth (int): Maximum recursion depth for content extraction.
        politeness_delay (float): Delay in seconds between HTTP requests to be polite
                                  to the server.

    Returns:
        tuple: A tuple containing:
            - list: A list of dictionaries, where each dictionary contains 'url' and 'text'
                    for the pages from which content was extracted.
            - list: A sorted list of all unique URLs discovered during the scrape.
    """
    global _visited_urls, _all_discovered_urls, _all_extracted_content

    # Clear global states for a fresh scrape
    _visited_urls.clear()
    _all_discovered_urls.clear()
    _all_extracted_content = []

    parsed_start_url = urlparse(start_url)
    base_url_netloc = parsed_start_url.netloc

    print(f"Starting web scraping from: {start_url}")
    print(f"Maximum pages for content extraction: {max_pages_to_visit}")
    print(f"Maximum recursion depth for content extraction: {max_depth}")
    print(f"Saving discovered URLs to: {output_filename}")
    print("\n--- Scraping in progress (this may take a while) ---\n")

    _recursive_scrape(base_url_netloc, start_url, max_depth, 0, 
                      max_pages_to_visit, politeness_delay)

    print("\n--- Scraping complete! ---\n")
    print(f"Total unique pages for which content was extracted: {len(_all_extracted_content)}")
    print(f"Total unique URLs discovered across the website: {len(_all_discovered_urls)}")

    # Save discovered URLs to a text file
    sorted_discovered_urls = sorted(list(_all_discovered_urls))
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            for url in sorted_discovered_urls:
                f.write(url + '\n')
        print(f"Successfully saved {len(sorted_discovered_urls)} URLs to {output_filename}")
    except IOError as e:
        print(f"Error saving URLs to file {output_filename}: {e}")

    return _all_extracted_content, sorted_discovered_urls

# --- Example Usage ---
if __name__ == "__main__":
    target_url = "https://www.mosdac.gov.in/"
    output_file = "mosdac_urls.txt"

    # Call the scraping function
    extracted_content, discovered_urls = scrape_website_to_file(
        start_url=target_url,
        output_filename=output_file,
        max_pages_to_visit=50,  # Adjust as needed
        max_depth=3,            # Adjust as needed
        politeness_delay=0.5    # Adjust as needed
    )

    print("\n--- Extracted Content (first 1000 chars per page) ---\n")
    # Print the extracted content for each page that was scraped.
    for i, page_data in enumerate(extracted_content):
        print(f"\n--- Content for Page {i+1}: {page_data['url']} ---")
        print(page_data['text'][:1000] + ('...' if len(page_data['text']) > 1000 else ''))
        print("-" * 50)
    
    print(f"\n--- Discovered URLs saved to {output_file} ---")
    print(f"You can find the complete list of URLs in the file: {os.path.abspath(output_file)}")

    print("\nImportant Note on Web Scraping:")
    print("Always check a website's robots.txt file (e.g., https://www.mosdac.gov.in/robots.txt) ")
    print("before scraping to understand their rules and policies. Be respectful of server load ")
    print("and avoid making too many requests in a short period.")

