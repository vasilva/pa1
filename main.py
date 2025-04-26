import argparse
import shutil
import os
from crawler import Crawler


def zip_warc_files(path: str):
    """
    Compress a directory into a zip file.

    Parameters
    ----------
        path (str): Path to the directory to compress.
    """
    sub_directories = [f.name for f in os.scandir(path) if f.is_dir()]
    for sub_dir in sub_directories:
        sub_dir_path = os.path.join(path, sub_dir)
        output_filename = f"zip/block {sub_dir}"
        shutil.make_archive(output_filename, "zip", sub_dir_path)


def get_args():
    """
    Parse command-line arguments.

    Returns
    -------
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Web Crawler",
        usage="%(prog)s -s <SEEDS> -n <LIMIT> [-d] [-l]",
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
        default=1000,
    )
    parser.add_argument(
        "-d",
        "--Debug",
        action="store_true",
        help="Enable debug mode",
    )
    parser.add_argument(
        "-l",
        "--Log",
        action="store_true",
        help="Enable Logging",
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
        log=args.Log,
        block_size=1000,
    )
    crawler.run()
    zip_warc_files("warc")


if __name__ == "__main__":
    main()
