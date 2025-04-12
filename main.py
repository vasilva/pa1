import argparse
from crawler import Crawler

seeds_urls = [
    "http://www.r7.com/",
    "http://mdemulher.abril.com.br/",
    "http://www.personare.com.br/",
]

LIMIT = 5


def get_args():
    """
    Parse command-line arguments.

    Returns
    -------
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Web Crawler",
        usage="%(prog)s -s <SEEDS> -n <LIMIT> [-d]",
    )
    parser.add_argument(
        "-s",
        "--Seeds",
        help="File containing seed URLs",
        type=str,
        default="seeds-2017023617.txt",
    )
    parser.add_argument(
        "-n",
        "--Limit",
        type=int,
        help="Maximum number of URLs to visit",
        default=LIMIT,
    )
    parser.add_argument(
        "-d",
        "--Debug",
        action="store_true",
        help="Enable debug mode",
    )
    return parser.parse_args()


def main():
    """
    Main function to run the web crawler.
    """
    args = get_args()
    seeds = args.Seeds
    with open(seeds, "r") as f:
        seeds = f.readlines()
        seeds = {url.strip() for url in seeds}

    crawler = Crawler(
        urls=seeds,
        max_urls=args.Limit,
        debug=args.Debug,
    )
    crawler.run()


if __name__ == "__main__":
    main()
