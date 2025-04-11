import requests
import json
import argparse
from bs4 import BeautifulSoup
from crawler import Crawler


# if __name__ == "__main__":
#     crawler = Crawler(start_urls=["https://www.r7.com/"], max_pages=10)
#     crawler.crawl()

print(("http://www.r7.com/").split("/"))