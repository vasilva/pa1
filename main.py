import requests
import json
import argparse
from bs4 import BeautifulSoup


def get_urls(seeds_file: str) -> list[str]:
    """
    Reads the seed URLs from a file.

    Parameters
    -----------
    seeds_file: str
        The path to the seeds file.

    Returns
    -------
    list[str]
        A list of seed URLs.
    """
    # Read the seed URLs from the file
    # Each line in the file should contain one URL
    try:
        with open(seeds_file, "r") as file:
            urls = [line.strip() for line in file.readlines()]
    except FileNotFoundError:
        print(f"Error: The file {seeds_file} does not exist.")
        return []

    return urls


def get_arguments() -> tuple[str, int, bool]:
    """
    Parses command-line arguments.

    Returns
    -------
    tuple
        A tuple containing:
        - str: The path to the seeds file.
        - int: The target number of webpages to be crawled.
        - bool: Whether to run in debug mode.
    """

    # Create the argument parser
    parser = argparse.ArgumentParser(
        description="Web Crawler",
        usage="python3 %(prog)s -s <SEEDS> -n <LIMIT> [-d]",
    )
    # Add command-line arguments
    parser.add_argument(
        "-s",
        "--Seeds",
        type=str,
        help="The path to a file containing a list of seed URLs (one URL per line) for initializing the crawling process.",
        required=True,
    )
    parser.add_argument(
        "-n",
        "--Number",
        type=int,
        help=" The target number of webpages to be crawled; the crawler should stop its execution once this target is reached.",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--Debug",
        help="Run in debug mode.",
        required=False,
        action="store_true",
    )
    args = parser.parse_args()

    return args.Seeds, args.Number, args.Debug


def main():

    # Parse command-line arguments
    seeds_file, target_number, debug_mode = get_arguments()

    # Check if the seeds file exists
    try:
        with open(seeds_file, "r") as file:
            pass
    except FileNotFoundError:
        print(f"Error: The file {seeds_file} does not exist.")
        return

    if debug_mode:
        print(
            f"Debug mode is enabled. Seeds file: {seeds_file}, Target number: {target_number}"
        )
    else:
        print(f"Seeds file: {seeds_file}, Target number: {target_number}")

    # Read the seed URLs from the file
    urls = get_urls(seeds_file)
    if debug_mode:
        print(f"Seed URLs: {urls}")
    # Initialize the list of crawled URLs
    crawled_urls = []
    # Initialize the list of URLs to crawl
    urls_to_crawl = urls.copy()
    if debug_mode:
        print(f"URLs to crawl: {urls_to_crawl}")


if __name__ == "__main__":
    main()
