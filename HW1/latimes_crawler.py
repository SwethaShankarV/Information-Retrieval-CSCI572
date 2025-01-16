import logging
import csv
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue

# Set up logging configuration
logging.basicConfig(
    format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
    level=logging.INFO
)

class Crawler:
    def __init__(self, base_url, urls=None, max_pages=20000, max_depth=16, max_workers=20):
        # Initialize the crawler with base URL and other parameters
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
        })  
        self.visited_urls = set()  # Set to track visited URLs
        self.urls_to_visit = Queue()  # Queue for managing URLs to visit
        if urls:
            for url in set(urls):  # Add initial URLs to the queue
                self.urls_to_visit.put((url, 1))
        self.max_pages = max_pages  # Maximum number of pages to fetch
        self.fetched_pages = 0  # Counter for fetched pages
        self.max_depth = max_depth  # Maximum depth for crawling
        self.site_name = self.get_site_name(base_url)  # Extract site name from base URL
        self.init_csv_files()  # Initialize CSV files for logging
        self.lock = threading.Lock()  # Lock for thread-safe operations
        self.executor = ThreadPoolExecutor(max_workers=max_workers)  # Thread pool for concurrent crawling
        self.stop_event = threading.Event()  # Event to signal when to stop crawling

    # -------------------- Helper Functions --------------------

    def get_site_name(self, url):
        # Extract the site name from the URL
        parsed = urlparse(url)
        domain_parts = parsed.netloc.split('.')
        if len(domain_parts) >= 2:
            return domain_parts[-2]  # Return the second last part of the domain
        return parsed.netloc

    def init_csv_files(self):
        # Initialize CSV files for logging fetched pages and visits
        self.fetch_file = open(f'fetch_{self.site_name}.csv', 'w', newline='', encoding='utf-8')
        self.visit_file = open(f'visit_{self.site_name}.csv', 'w', newline='', encoding='utf-8')
        self.urls_file = open(f'urls_{self.site_name}.csv', 'w', newline='', encoding='utf-8')

        # Create CSV writers for each file
        self.fetch_writer = csv.writer(self.fetch_file)
        self.visit_writer = csv.writer(self.visit_file)
        self.urls_writer = csv.writer(self.urls_file)

        # Write header rows for each CSV file
        self.fetch_writer.writerow(['URL', 'Status'])
        self.visit_writer.writerow(['URL', 'Size (Bytes)', '# of Outlinks', 'Content-Type'])
        self.urls_writer.writerow(['URL', 'Indicator'])

        # Locks for writing to CSVs to ensure thread safety
        self.fetch_lock = threading.Lock()
        self.visit_lock = threading.Lock()
        self.urls_lock = threading.Lock()

    def download_url(self, url):
        # Download the content of a URL and return relevant information
        try:
            response = self.session.get(url, timeout=10)  # Send GET request to the URL
            content_type = response.headers.get('Content-Type', '')  # Get the Content-Type header
            if any(ct in content_type.lower() for ct in ['html', 'pdf', 'msword', 'image']):
                return response.text, response.status_code, len(response.content), content_type, False
            else:
                return '', response.status_code, 0, content_type, True  # Skip irrelevant content types
        except Exception as e:
            logging.exception(f'Error downloading {url}: {e}')  # Log any exceptions
            return '', 0, 0, '', True  # Return defaults on error

    # -------------------- Crawling Functionality --------------------

    def crawl(self, url, depth):
        # Main crawling function to fetch a URL and process it
        with self.lock:
            if self.stop_event.is_set():  # Check if crawling should stop
                return
            if self.fetched_pages >= self.max_pages:  # Check if max pages limit is reached
                self.stop_event.set()
                return
            if url in self.visited_urls:  # Check if the URL has already been visited
                return
            
            # Mark the URL as visited and increment the fetched pages count
            self.visited_urls.add(url)
            self.fetched_pages += 1
            current_fetched = self.fetched_pages

        logging.info(f'Crawling: {url} at depth {depth} (Fetched: {current_fetched})')
        html, status_code, size, content_type, skip = self.download_url(url)  # Download the URL

        # Skip processing for status codes 0 and 999
        if status_code in [0, 999]:
            return

        # Write to fetch.csv
        with self.fetch_lock:
            self.fetch_writer.writerow([url, status_code])  # Log the fetched URL and its status

        if not skip and status_code == 200:  # Process only if content is valid
            outlinks = list(self.get_linked_urls(url, html))  # Get all linked URLs from the page
            unique_outlinks = set(outlinks)  # Create a set for unique outlinks

            # Write visit information to visit.csv
            with self.visit_lock:
                self.visit_writer.writerow([url, size, len(unique_outlinks), content_type])

            # Write all discovered URLs to urls.csv, including duplicates
            with self.urls_lock:
                for outlink in outlinks:
                    indicator = 'OK' if self.is_internal_url(outlink) else 'N_OK'  # Mark as OK or N_OK
                    self.urls_writer.writerow([outlink, indicator])

            # Enqueue new unique links for crawling if depth limit not reached
            if depth < self.max_depth:
                for outlink in unique_outlinks:
                    with self.lock:
                        if self.fetched_pages >= self.max_pages:  # Check if max pages limit is reached
                            self.stop_event.set()
                            break
                        if outlink not in self.visited_urls:  # Add only unvisited URLs
                            self.urls_to_visit.put((outlink, depth + 1))

    def is_internal_url(self, url):
        # Check if a URL is internal to the base domain
        return urlparse(url).netloc == urlparse(self.base_url).netloc

    def get_linked_urls(self, url, html):
        # Extract all linked URLs from the HTML content
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a', href=True):  # Find all anchor tags with href attributes
            path = link.get('href')
            if path.startswith('/'):  # Handle relative URLs
                path = urljoin(url, path)  # Convert to absolute URL
            elif not path.startswith('http'):  # Ignore non-http URLs
                continue
            yield path  # Yield the absolute URL

    # -------------------- Crawler Execution --------------------

    def run(self):
        # Start the crawling process
        try:
            futures = set()  # Set to hold futures for tracking submitted tasks
            # Submit initial batch of URLs for crawling
            while not self.urls_to_visit.empty() and self.fetched_pages < self.max_pages:
                url, depth = self.urls_to_visit.get()
                future = self.executor.submit(self.crawl, url, depth)  # Submit the crawl task
                futures.add(future)

            # Process completed futures and submit new ones
            while futures and not self.stop_event.is_set():
                # as_completed yields futures as they complete
                for future in as_completed(futures):
                    futures.remove(future)  # Remove completed future from the set
                    try:
                        future.result()  # Get the result of the completed future
                    except Exception as e:
                        logging.exception(f'Error processing a future: {e}')

                    # Submit new URLs from the queue
                    while not self.urls_to_visit.empty() and self.fetched_pages < self.max_pages and not self.stop_event.is_set():
                        url, depth = self.urls_to_visit.get()
                        future = self.executor.submit(self.crawl, url, depth)  # Submit new crawl task
                        futures.add(future)
        finally:
            self.shutdown()  # Ensure resources are cleaned up

    def shutdown(self):
        # Clean up resources on shutdown
        self.executor.shutdown(wait=True)  # Shutdown the thread pool
        self.fetch_file.close()  # Close the fetch CSV file
        self.visit_file.close()  # Close the visit CSV file
        self.urls_file.close()  # Close the URLs CSV file
        self.session.close()  # Close the session
        logging.info('Crawler has been shut down gracefully.')  # Log shutdown message

# -------------------- Main Execution --------------------

if __name__ == '__main__':
    Crawler(base_url='https://www.latimes.com', urls=['https://www.latimes.com']).run()
