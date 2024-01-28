import os
import sqlite3
import time
import requests
import concurrent.futures
import argparse
import logging
from urllib.parse import urlparse
from typing import Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class URLProcessor:
    def __init__(self, package: str, output: str, max_workers: int):
        self.package = package
        self.database_name = 'urls.db'
        self.output = output
        self.max_workers = max_workers

    def process_urls(self, grep_output_file: str):
        self._save_urls_to_db(grep_output_file)
        self._remove_invalid_rows()
        self._rename_duplicates()
        self._download_files()
        self._cleanup(grep_output_file)

    def _save_urls_to_db(self, grep_output_file: str):
        assert not grep_output_file.startswith(self.package)
        grep_command = f"grep -rEho 'https://cdn\\.discordapp\\.com/[^ ]+' {self.package} > {grep_output_file}"
        os.system(grep_command)
        with open(grep_output_file, 'r') as file:
            grep_output = file.read().splitlines()
        with sqlite3.connect(self.database_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS urls
                            (url text, filename_from_url text)''')
            for line in grep_output:
                if line:
                    url = line
                    filename_from_url = url.split('/')[-1].split('?')[0]
                    cursor.execute("INSERT INTO urls VALUES (?, ?)", (url, filename_from_url))

    def _remove_invalid_rows(self):
        with sqlite3.connect(self.database_name) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM urls WHERE filename_from_url LIKE '%,'")

    def _rename_duplicates(self):
        with sqlite3.connect(self.database_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT filename_from_url FROM urls")
            filenames = [row[0] for row in cursor.fetchall()]
            filename_counts = {}
            for filename in filenames:
                if filename not in filename_counts:
                    filename_counts[filename] = 0
                filename_counts[filename] += 1
            for filename, count in filename_counts.items():
                if count > 1:
                    for i in range(1, count):
                        old_filename = filename
                        new_filename = f"{str(i).zfill(4)}_{filename}"
                        cursor.execute("UPDATE urls SET filename_from_url = ? WHERE filename_from_url = ? AND rowid = (SELECT MIN(rowid) FROM urls WHERE filename_from_url = ?)", (new_filename, old_filename, old_filename))

    def _download_files(self):
        with sqlite3.connect(self.database_name) as conn:
            cursor = conn.cursor()
            cursor.execute("ALTER TABLE urls ADD COLUMN success INTEGER DEFAULT 0")
            cursor.execute("ALTER TABLE urls ADD COLUMN has_failed INTEGER DEFAULT 0")
            cursor.execute("SELECT url, filename_from_url FROM urls")
            urls_and_filenames = cursor.fetchall()
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for i, (url, filename) in enumerate(urls_and_filenames, start=1):
                    file_extension = os.path.splitext(filename)[1].lower()  # Convert file extension to lowercase
                    subdirectory = os.path.join(self.output, file_extension.lstrip('.'))
                    os.makedirs(subdirectory, exist_ok=True)
                    success = executor.submit(self._download_file, url, filename, subdirectory).result()
                    if success:
                        cursor.execute("UPDATE urls SET success = 1 WHERE url = ?", (url,))
                    else:
                        cursor.execute("UPDATE urls SET has_failed = 1 WHERE url = ?", (url,))
                    logging.info(f"Downloaded {i} of {len(urls_and_filenames)} files: {filename} (success: {success})")

    @staticmethod
    def _download_file(url: str, filename: str, subdirectory: str) -> bool:
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(os.path.join(subdirectory, filename), 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            return True
        except (requests.exceptions.RequestException, IOError):
            return False

    def _cleanup(self, grep_output_file: str):
        os.remove(grep_output_file)
        os.remove(self.database_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Grabs all attachment-looki URLs from a discord data package and downloads the files.')
    parser.add_argument('--package', required=True, help='Path of extracted package.')
    parser.add_argument('--output', required=True, help='Place to dump the files')
    parser.add_argument('--max_workers', type=int, default=3, help='Worker threads.')
    args = parser.parse_args()
    processor = URLProcessor(args.package, args.output, args.max_workers)
    processor.process_urls('grep_output.txt')
