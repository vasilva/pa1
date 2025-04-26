import logging
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from protego import Protego
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
from io import BytesIO
import requests
from requests.compat import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import cpu_count, makedirs, path
from bs4 import XMLParsedAsHTMLWarning
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

max_n_threads = cpu_count()

logging.basicConfig(
    # filename="crawler.log",
    filemode="w",
    format="%(asctime)s,%(msecs)03d\n" + "%(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

json_format = {
    "URL": "",
    "Title": "",
    "Text": "",
    "Timestamp": 0,
}

# Constants
# Maximum number of words to extract from the page
MAX_TEXT_SIZE = 20
# Maximum time to wait for a response
TIMEOUT = 2
# Maximum time to wait between requests
WAIT_TIME = 100  # in milliseconds
# Maximum number of threads to use
MAX_THREADS = max_n_threads - 1


def url_to_filename(url: str) -> str:
    """
    Convert a URL to a filename by replacing invalid characters.

    Parameters
    ----------
        url (str): The URL to convert.
    Returns
    -------
        str: The converted filename.
    """
    # Replace invalid characters with underscores
    filename = url.replace(":", ";").replace("/", "_").replace("?", "!")
    return filename


def print_json_text(url: str, soup: BeautifulSoup) -> None:
    """
    Print the JSON format of the page content.
    This function is used for debugging purposes.

    Parameters
    ----------
        url (str): The URL of the page.
        soup (BeautifulSoup): The BeautifulSoup object containing the HTML content.

    Example
    -------
    The JSON format will look like this:

    {
        "URL": "http://example.com",
        "Title": "Example Title",
        "Text": "The first 20 words of the page.",
        "Timestamp": 1234567890
    }
    """
    # Extract text from the page
    timestamp = int(time.time())
    title_tag = soup.find("title")
    title = title_tag.text if title_tag else ""

    text_tag = soup.get_text()
    text = text_tag.split() if text_tag else []
    # Limit the text to the first 20 words
    text = " ".join(text[:MAX_TEXT_SIZE]) if text else ""

    # Create a JSON-like format
    json_format["URL"] = url
    json_format["Title"] = title
    json_format["Text"] = text
    json_format["Timestamp"] = timestamp

    # Print the JSON format
    print(json_format, end="\n\n")


class CrawlerException(Exception):
    """Exception for the Crawler class."""


class Crawler:
    """
    A simple web crawler that visits a list of URLs and extracts links from them.
    """

    def __init__(
        self,
        urls: set[str] = set(),
        max_urls: int = 100,
        max_depth: int = 10,
        debug: bool = False,
        log: bool = False,
        block_size: int = 10,
    ) -> None:
        """
        Initialize the crawler with a list of URLs to visit.

        Parameters
        ----------
            urls (set[str]): A list of URLs to start crawling from.
            max_depth (int): The maximum depth to crawl.
            max_urls (int): The maximum number of URLs to visit.
            debug (bool): If True, enable debug mode.
            log (bool): If True, enable logging mode.
            n_blocks (int): The number of blocks to divide the URLs into.
            block_size (int): The size of each block.
        """
        # URLs to visit
        self.urls_to_visit = urls
        # URLs already visited
        self.urls_visited = set()
        # URLs disallowed by robots.txt
        self.urls_disallowed = set()
        # robots.txt files for each domain
        self.robots_txt = {}
        # Maximum number of URLs to visit
        self.max_urls = max_urls
        # Maximum depth to crawl
        self.max_depth = max_depth
        # Current depth of the crawl
        self.current_depth = 0
        # Debug mode
        self.debug = debug
        # Logging mode
        self.log = log
        # Size of each block of URLs
        if block_size > max_urls:
            self.block_size = max_urls
        else:
            self.block_size = block_size
        # Counter of the current block
        self.current_block = 0
        # Number of URLs downloaded
        self.urls_downloaded = 0

    def get_base_url(self, url: str) -> str:
        """
        Extract the base URL from a given URL.

        Parameters
        ----------
            url (str): The URL to extract the base URL from.
        Returns
        -------
            str: The base URL.
        """
        if not url.startswith("http"):
            return ""
        split_url = url.split("/")
        base_url = split_url[0] + "//" + split_url[2]
        return base_url

    def get_robots_txt(self, url: str) -> str:
        """
        Get the robots.txt file for a given URL.

        Parameters
        ----------
            url (str): The URL to get the robots.txt file for.
        Returns
        -------
            str: The content of the robots.txt file.
        """
        base_url = self.get_base_url(url)
        if base_url not in self.robots_txt:
            robots_txt = self.download_robots_txt(url)
            self.robots_txt[base_url] = robots_txt

        return self.robots_txt[base_url]

    def download_robots_txt(self, url: str) -> str:
        """
        Download the robots.txt file from the given URL.

        Parameters
        ----------
            url (str): The URL to download the robots.txt file from.
        Returns
        -------
            str: The content of the robots.txt file.
        """
        base_url = self.get_base_url(url)
        if not base_url:
            return ""

        # Download the robots.txt file
        try:
            robots_txt = requests.get(urljoin(base_url, "/robots.txt"), timeout=TIMEOUT)

        except requests.exceptions.RequestException:
            return ""
        except requests.exceptions.HTTPError:
            return ""
        except Exception:
            return ""

        # Check if the response is successful
        if robots_txt.status_code == 200:
            return robots_txt.text

        else:
            return ""

    def download_url(self, url: str):
        """
        Download the content of a URL.

        Parameters
        ----------
            url (str): The URL to download.
        Returns
        -------
            str: The content of the URL.
        """
        try:
            response = requests.get(url, timeout=TIMEOUT)

        except Exception:
            return ""

            # Check if the response is successful
        if response.status_code != 200:
            return ""

        self.current_block = self.urls_downloaded // self.block_size
        self.urls_downloaded += 1
        write_warc_file(url, response, self.current_block)
        return response.text

    def is_url_allowed(self, url: str) -> bool:
        """
        Check if a URL is allowed to be crawled based on the robots.txt file.

        Parameters
        ----------
            url (str): The URL to check.
        Returns
        -------
            bool: True if the URL is allowed, False otherwise.
        """
        if url in self.urls_disallowed:
            return False

        robots_txt = self.get_robots_txt(url)
        if not robots_txt:
            return True

        # Parse the robots.txt file
        rp = Protego.parse(robots_txt)

        # Check if the URL is allowed
        allowed = rp.can_fetch(url, "*")
        if not allowed:
            self.urls_disallowed.add(url)
        return allowed

    def get_linked_urls(self, url: str, html: str):
        """
        Extract all linked URLs from the HTML content of a page.

        Parameters
        ----------
            url (str): The URL of the page.
            html (str): The HTML content of the page.
        Yields
        ------
            str: A linked URL.
        """
        soup = BeautifulSoup(html, "html.parser")
        for link in soup.find_all("a"):
            href = link.get("href")
            # Skip if the link is not a valid URL
            if not href or not href.startswith(("http", "/")):
                continue
            # Convert relative URLs to absolute URLs
            if href.startswith("/"):
                href = urljoin(url, href)

            yield href

    def add_urls_to_visit(self, url: str) -> None:
        """
        Add a URL to the list of URLs to visit if it hasn't been visited yet.

        Parameters
        ----------
            url (str): The URL to add.
        """
        # Check if the URL is allowed to be crawled
        if not self.is_url_allowed(url):
            return

        # Check if the URL is already visited or disallowed
        if (
            len(self.urls_to_visit) < 10000
            and url not in self.urls_visited
            and url not in self.urls_disallowed
        ):
            self.urls_to_visit.add(url)

    def crawl(self, url: str) -> str:
        """
        Crawl a URL, download its content, and extract linked URLs.

        Parameters
        ----------
            url (str): The URL to crawl.

        Returns
        -------
            str: The HTML content of the page.
        """
        self.get_robots_txt(url)
        # Check if the URL is allowed to be crawled
        if not self.is_url_allowed(url):
            return ""

        # Check if the maximum depth is reached
        self.current_depth += 1
        if self.current_depth > self.max_depth:
            self.current_depth = 0
            return ""

        # Download the content of the URL
        html = self.download_url(url)

        # Check if the HTML content is valid
        if html:
            if self.debug:
                soup = BeautifulSoup(html, "html.parser")
                print_json_text(url, soup)

            # Extract linked URLs and add them to the list of URLs to visit
            for url in self.get_linked_urls(url, html):
                self.add_urls_to_visit(url)

        return html

    def crawl_thread(self, url: str, previous_url: str) -> str:
        """
        Crawl a URL in a separate thread.

        Parameters
        ----------
            url (str): The URL to crawl.
            previous_url (str): The previous URL visited.
        Returns
        -------
            str: The HTML content of the page.
        """
        if not url:
            return ""

        html = ""
        domain = self.get_base_url(url)
        if domain == self.get_base_url(previous_url):
            time.sleep(WAIT_TIME / 1000)

        try:
            html = self.crawl(url)

        except CrawlerException:
            return ""

        finally:
            self.urls_visited.add(url)
            return html

    def run(self) -> None:
        """
        Start the crawling process.
        """
        current_urls = ["" for _ in range(MAX_THREADS)]
        previous_urls = ["" for _ in range(MAX_THREADS)]
        results = []
        while self.urls_to_visit and self.urls_downloaded < self.max_urls:
            for i in range(MAX_THREADS):
                if not self.urls_to_visit:
                    break
                url = self.urls_to_visit.pop()
                current_urls[i] = url

            # Distribute to the threads
            with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                futures = {
                    executor.submit(self.crawl_thread, current_url, previous_url)
                    for current_url, previous_url in zip(current_urls, previous_urls)
                }
                for future in as_completed(futures):
                    try:
                        rs = future.result()
                        if rs:
                            results.append(rs)

                    except Exception as e:
                        if self.log:
                            logging.exception(f"Error in thread: {e}")

                    finally:
                        if len(results) >= self.block_size:
                            if self.log:
                                logging.info(f"Finished block")
                            # Clear the results
                            results.clear()

            # Update the previous URLs
            previous_urls = current_urls
            current_urls = ["" for _ in range(MAX_THREADS)]
            if self.log:
                print("-" * 50)
                print(f"Downloaded {self.urls_downloaded} URLs.")


def write_warc_file(url: str, response: requests.Response, block: int):
    """
    Write the HTML content to a WARC file.

    Parameters
    ----------
        url (str): The URL of the page.
        response (requests.Response): The response object containing the content.
        block (int): The block number.
    """
    # Create the WARC file
    filename = f"warc/{block}/{url_to_filename(url)}.warc.gz"
    # Create the directory if it doesn't exist
    makedirs(path.dirname(filename), exist_ok=True)

    with open(filename, "wb") as f:
        writer = WARCWriter(f, gzip=True)

        # get raw headers from urllib3
        headers_list = response.raw.headers.items()

        http_headers = StatusAndHeaders("200 OK", headers_list, protocol="HTTP/1.1")

        record = writer.create_warc_record(
            url,
            "response",
            payload=BytesIO(response.content),
            http_headers=http_headers,
        )
        writer.write_record(record)
