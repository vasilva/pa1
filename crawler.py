import logging
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests
from requests.compat import urljoin


logging.basicConfig(
    # filename="crawler.log",
    filemode="w",
    format="%(asctime)s,%(msecs)03d\n    %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


class Crawler:
    """
    A simple web crawler that visits a list of URLs and extracts links from them.
    """

    def __init__(self, urls=[], max_urls=100):
        """
        Initialize the crawler with a list of URLs to visit.

        Parameters
        ----------
            urls (list[str]): A list of URLs to start crawling from.
            max_urls (int): The maximum number of URLs to visit.
        """
        self.urls_visited = set()
        self.urls_to_visit = urls
        self.max_urls = max_urls

    def download_url(self, url):
        """
        Download the content of a URL.

        Parameters
        ----------
            url (str): The URL to download.
        Returns
        -------
            str: The HTML content of the URL.
        """
        response = requests.get(url)
        if response.status_code != 200:
            logging.exception(f"Failed to download: {url}")
            return None

        logging.info(f"Downloaded: {url}")
        with open(f"out/{len(self.urls_visited)}.txt", "w") as f:
            f.write(response.text)
        # Return the HTML content
        return response.text

    def get_linked_urls(self, url, html):
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
            if href and href.startswith("/"):
                href = urljoin(url, href)
            yield href

    def add_urls_to_visit(self, url):
        """
        Add a URL to the list of URLs to visit if it hasn't been visited yet.

        Parameters
        ----------
            url (str): The URL to add.
        """
        if url not in self.urls_visited and url not in self.urls_to_visit:
            self.urls_to_visit.append(url)

    def crawl(self, url):
        """
        Crawl a URL, download its content, and extract linked URLs.

        Parameters
        ----------
            url (str): The URL to crawl.
        """
        html = self.download_url(url)
        for url in self.get_linked_urls(url, html):
            self.add_urls_to_visit(url)

    def run(self):
        """
        Start the crawling process.
        """
        while self.urls_to_visit and len(self.urls_visited) < self.max_urls:
            url = self.urls_to_visit.pop(0)
            logging.info(f"Crawling: {url}")
            try:
                self.crawl(url)
            except Exception:
                logging.exception(f"Failed to crawl: {url}")
            finally:
                self.urls_visited.add(url)


crawler = Crawler(
    urls=[
        "http://www.r7.com/",
        "http://mdemulher.abril.com.br/",
        "http://www.personare.com.br/",
    ],
    max_urls=20,
)

crawler.run()
print(len(crawler.urls_visited))
