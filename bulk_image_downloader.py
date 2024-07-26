"""
Image downloader from the links fetched by link_extractor.py
Uses mutli-threading for faster downloads

TODO:
    - change the naming to include the coordinates
    - change saving links from text file to csv file to include information on coordinates
    - refactor link_extractor.py
"""

import os
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

def download_image(url, output_dir, index):
    """
    Download a single image from the URL and save it to the output directory with a sequential name.
    """
    try:
        #print(f"Attempting to download: {url}")  # Debug print
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check for request errors

        # Define filename using the index
        filename = os.path.join(output_dir, f'img{index}.tif')

        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded: {filename}")
    except requests.RequestException as e:
        print(f"Failed to download {url}: {e}")

def process_downloads(file_path, output_dir, max_workers=10):
    """
    Process the download of images listed in the file_path using multi-threading.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Read URLs from file
    with open(file_path, 'r') as file:
        urls = file.read().splitlines()

    print(f"Total URLs to download: {len(urls)}")  # Debug print

    # Download images with progress bar
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(total=len(urls), desc="Downloading Images") as pbar:
            futures = []
            for index, url in enumerate(urls, start=1):
                future = executor.submit(download_image, url, output_dir, index)
                future.add_done_callback(lambda f: pbar.update(1))
                futures.append(future)

            # Wait for all futures to complete
            for future in futures:
                future.result()

if __name__ == "__main__":
    # File path to the text file with download links
    links_file = 'output_images/download_links.txt'
    # Directory to save downloaded images
    output_directory = 'output_images/downloaded_images/'
    # Number of parallel threads for downloading
    num_threads = 10

    process_downloads(links_file, output_directory, max_workers=num_threads)

