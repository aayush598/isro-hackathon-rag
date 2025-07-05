import os
from bs4 import BeautifulSoup

def extract_text_from_html_files_and_save(html_files_directory, output_directory):
    """
    Extracts text content from all HTML files in a given directory
    and saves each extracted text to a separate file in the output directory.

    Args:
        html_files_directory (str): The path to the directory containing HTML files.
        output_directory (str): The path to the directory where extracted text files will be saved.
    """
    if not os.path.isdir(html_files_directory):
        print(f"Error: HTML files directory '{html_files_directory}' not found. 😟")
        return

    # Create the output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    print(f"✅ Output directory '{output_directory}' ensured.")

    print(f"🚀 Processing HTML files in: {html_files_directory}")
    for filename in os.listdir(html_files_directory):
        if filename.endswith(".html") or filename.endswith(".htm"):
            filepath = os.path.join(html_files_directory, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    html_content = f.read()

                soup = BeautifulSoup(html_content, 'html.parser')
                # Get all the text from the document, excluding script and style tags
                text_content = soup.get_text(separator='\n', strip=True)

                # Determine the output filename (e.g., "original_file.html" -> "original_file.txt")
                output_filename = os.path.splitext(filename)[0] + ".txt"
                output_filepath = os.path.join(output_directory, output_filename)

                with open(output_filepath, 'w', encoding='utf-8') as outfile:
                    outfile.write(text_content)
                print(f"✨ Extracted content from '{filename}' and saved to: '{output_filepath}'")

            except Exception as e:
                print(f"❌ Error processing '{filename}': {e}")

# Specify the directory where your HTML files are stored
html_directory = "mosdac_downloaded_content/html_pages"
# Specify the directory where you want to save the extracted text files
output_text_directory = "mosdac_extracted_text"

# Run the function to extract and save the content
extract_text_from_html_files_and_save(html_directory, output_text_directory)