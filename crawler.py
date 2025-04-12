import logging
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from protego import Protego
import requests
from requests.compat import urljoin


logging.basicConfig(
    filename="crawler.log",
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
TIMEOUT = 5  # in seconds
# Maximum time to wait between requests
WAIT_TIME = 100  # in milliseconds


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
        max_depth: int = 20,
        debug: bool = False,
    ):
        """
        Initialize the crawler with a list of URLs to visit.

        Parameters
        ----------
            urls (set[str]): A list of URLs to start crawling from.
            max_depth (int): The maximum depth to crawl.
            max_urls (int): The maximum number of URLs to visit.
            debug (bool): If True, enable debug mode.
        """
        self.urls_to_visit = urls
        self.urls_visited = set()
        self.urls_disallowed = set()
        self.robots_txt = {}
        self.max_urls = max_urls
        self.max_depth = max_depth
        self.current_depth = 0
        self.debug = debug

    def get_base_url(self, url: str):
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

    def get_robots_txt(self, url: str):
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

    def download_robots_txt(self, url: str):
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
            if self.debug:
                logging.exception(f"Invalid URL: {url}")
            return ""
        
        # Download the robots.txt file
        try:
            robots_txt = requests.get(urljoin(base_url, "/robots.txt"), timeout=TIMEOUT)
        
        except requests.exceptions.RequestException:
            if self.debug:
                logging.exception(f"Failed to download robots.txt: {url}")
            return ""
        
        except requests.exceptions.HTTPError:
            if self.debug:
                logging.exception(f"Failed to download robots.txt: {url}")
            return ""
        
        # Check if the response is successful
        if robots_txt.status_code == 200:
            if self.debug:
                logging.info(f"Downloaded robots.txt: {base_url}")
            return robots_txt.text
        else:
            if self.debug:
                logging.exception(f"Failed to download robots.txt: {base_url}")
            return ""

    def download_url(self, url: str):
        """
        Download the content of a URL.

        Parameters
        ----------
            url (str): The URL to download.
        Returns
        -------
            str: The HTML content of the URL.
        """
        try:
            response = requests.get(url, timeout=TIMEOUT)

        except requests.exceptions.RequestException:
            if self.debug:
                logging.exception(f"Failed to download: {url}")
            return ""
        
        except requests.exceptions.HTTPError:
            if self.debug:
                logging.exception(f"Failed to download: {url}")
            return ""

        # Check if the response is successful
        if response.status_code != 200:
            if self.debug:
                logging.exception(f"Failed to download: {url}")
            return ""

        if self.debug:
            logging.info(f"Downloaded: {url}")
            with open(f"out/{len(self.urls_visited)}.txt", "w") as f:
                f.write(response.text)

        return response.text

    def is_url_allowed(self, url: str):
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
            if self.debug:
                logging.info(f"URL disallowed by robots.txt: {url}")
            self.urls_disallowed.add(url)
        return allowed

    def print_json_text(self, url: str, soup: BeautifulSoup):
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

    def add_urls_to_visit(self, url: str):
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
        if url not in self.urls_visited and url not in self.urls_disallowed:
            self.urls_to_visit.add(url)

    def crawl(self, url: str):
        """
        Crawl a URL, download its content, and extract linked URLs.

        Parameters
        ----------
            url (str): The URL to crawl.
        """
        self.get_robots_txt(url)
        # Check if the URL is allowed to be crawled
        if not self.is_url_allowed(url):
            if self.debug:
                logging.info(f"URL disallowed: {url}")
            return

        # Check if the maximum depth is reached
        self.current_depth += 1
        if self.debug:
            logging.info(f"Current depth: {self.current_depth}")
        if self.current_depth > self.max_depth:
            if self.debug:
                logging.info(f"Maximum depth reached: {url}")
            self.current_depth = 0
            return

        # Download the content of the URL
        html = self.download_url(url)

        # Check if the HTML content is valid
        if not html:
            return

        if self.debug:
            soup = BeautifulSoup(html, "html.parser")
            self.print_json_text(url, soup)

        # Extract linked URLs and add them to the list of URLs to visit
        for url in self.get_linked_urls(url, html):
            self.add_urls_to_visit(url)

    def run(self):
        """
        Start the crawling process.
        """
        previous_url = ""
        while self.urls_to_visit and len(self.urls_visited) < self.max_urls:
            url = self.urls_to_visit.pop()
            domain = self.get_base_url(url)
            if domain == self.get_base_url(previous_url):
                time.sleep(WAIT_TIME / 1000)
            
            if self.debug:
                logging.info(f"Crawling: {url}")
            
            try:
                self.crawl(url)

            except CrawlerException:
                if self.debug:
                    logging.exception(f"Failed to crawl: {url}")

            finally:
                self.urls_visited.add(url)
                previous_url = url

        print(f"Visited {len(self.urls_visited)} URLs.")
        print(f"Disallowed {len(self.urls_disallowed)} URLs.")
        print(f"Remaining {len(self.urls_to_visit)} URLs.")
