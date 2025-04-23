import shutil
import os

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
        output_filename = f"zip/Bloco {sub_dir}"
        shutil.make_archive(output_filename, "zip", sub_dir_path)
        print(f"Compressing {sub_dir_path} in {output_filename}.zip")

zip_warc_files("warc")