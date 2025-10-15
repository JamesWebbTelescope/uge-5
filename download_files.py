import threading
import traceback
import pandas as pd
import requests
from pathlib import Path
import glob
import os
import time
import concurrent.futures

from prepare import PreparePdfDownloader
from report_writer import ReportWriter

class PDFDownloader:
    def __init__(self, list_path, output_folder, main_col, secondary_col, id_col="BRnum"): # constructor
        self.list_path = list_path
        self.output_folder = Path(output_folder)
        self.dwn_folder = self.output_folder / "dwn"
        self.main_col = main_col
        self.secondary_col = secondary_col
        self.id_col = id_col
        self.df = None
        self.df2 = None

    
    def download_pdf(self, url: str, savepath: Path) -> tuple[bool, str]:
        """
        Download a PDF file from a URL and save it to a specified path.

        This method attempts to make a GET request to the provided URL. If the request is successful (HTTP status code 200),
        it writes the content to the specified save path. If the request fails, it returns an error message.

        Args:
            url (str): the URL of the PDF file to download
            savepath (Path): the path where the PDF file should be saved

        Returns:
          Tuple: (success: bool, error_message: str).

        Raises:
            requests.exceptions.Timeout: If the request times out.
            requests.exceptions.ConnectionError: If there are connection issues.
            requests.exceptions.HTTPError: If the HTTP request returned an unsuccessful status code.
            requests.exceptions.ChunkedEncodingError: If there are issues with chunked encoding.
            IOError: If there are issues writing the file to disk.
            requests.exceptions.RequestException: For any request-related errors.
            Exception: For any other unexpected errors.

        """
        try:
            response = requests.get(url, stream=True, timeout=10) # make a GET request

            if response.status_code != 200:
                return False, f"Failed to download: {response.status_code} - {response.reason}"

            total = int(response.headers.get('content-length', 0)) # total file size to download in bytes
            downloaded = 0

            with open(savepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192): # downloads 8KB chunks
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)
                        downloaded += len(chunk)
                        print(f"Downloaded {downloaded}/{total} bytes", end="\r")

            return True, ""

        except requests.exceptions.Timeout as te:
            return False, f"Request timed out: {str(te)}"
        except requests.exceptions.ConnectionError as ce:
            return False, f"Connection error: {str(ce)}"
        except requests.exceptions.HTTPError as he:
            return False, f"HTTP error: {str(he)}"
        except requests.exceptions.ChunkedEncodingError as ce:
            return False, f"Chunked encoding error: {str(ce)}"
        except IOError as ioe:
            return False, f"I/O error: {str(ioe)}"
        except requests.exceptions.RequestException as re:
            return False, f"Request error: {str(re)}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"

    def process_downloads_threaded(self, number_of_files: int = 10, max_workers: int = 4) -> None:
        """
        Download multiple PDF files concurrently using multithreading.

        Attempts to download up to `number_of_files` PDFs from the DataFrame using ThreadPoolExecutor.
        If the download from the main URL fails, it retries with the secondary URL.
        Errors are recorded in the DataFrame.

        Args:
            number_of_files (int): Number of files to attempt downloading.
            max_workers (int): Maximum number of threads for concurrent downloads.

        Prints:
            Total time taken and number of files processed.

        Raises:
            KeyError: If required columns are missing in the DataFrame.
            Exception: For any other unexpected errors during the download process.
        """
        start_time = time.perf_counter()
        lock = threading.Lock() # Ensure thread-safe write operations. by locking a section of code to be executed by only one thread at a time.

        def download_task(br_number):
            try:
                savefile = self.dwn_folder / f"{br_number}.pdf"
                url_main = self.df2.at[br_number, self.main_col]
                url_secondary = self.df2.at[br_number, self.secondary_col]

                print(f"Downloading {br_number} from {url_main} ...")
                success, error = self.download_pdf(url_main, savefile)

                if not success:
                    with lock:
                        self.df2.at[br_number, "error"] = error

                    success2, error2 = self.download_pdf(url_secondary, savefile)
                    if not success2:
                        with lock:
                            self.df2.at[br_number, "error"] = error2
                        result = "Not downloaded" # both downloads failed
                    else:
                        result = "Downloaded" # secondary download succeeded
                else:
                    result = "Downloaded" # main download succeeded

                with lock: 
                    report_writer.write_to_report(str(br_number), result, output_folder=self.output_folder)

            except KeyError as e:
                with lock:
                    self.df2.at[br_number, "error"] = f"Missing column: {e}"
                print(f"KeyError for {br_number}: {e}")
            except Exception as e:
                with lock:
                    self.df2.at[br_number, "error"] = f"Unexpected error: {e}"
                print(f"Unexpected error for {br_number}: {traceback.format_exc()}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor: # Use ThreadPoolExecutor for multithreading
            executor.map(download_task, self.df2.index[:number_of_files]) #TODO Handle AttributeError for NoneType

        end_time = time.perf_counter()
        print(f"Downloaded {min(number_of_files, len(self.df2))} files using {max_workers} threads in {end_time - start_time:.2f} seconds.")


    def delete_downloaded_files(self) -> None:
        """
        Delete all downloaded PDF files in the download folder.
        Logs errors for individual files and continues deleting others.
        Prints the number of files deleted and any errors encountered.

        Raises:
            PermissionError: If there are permission issues deleting a file.
            FileNotFoundError: If a file to be deleted is not found.
            OSError: For other OS-related errors during file deletion.
            Exception: For any other unexpected errors.
        """
        try:
            dwn_files = glob.glob(str(self.dwn_folder / "*.pdf"))
            deleted_count = 0
            for f in dwn_files:
                try:
                    os.remove(f)
                    deleted_count += 1
                except PermissionError as pe:
                    print(f"Permission denied when deleting {f}: {pe}")
                except FileNotFoundError as fnf:
                    print(f"File not found when deleting {f}: {fnf}")
                except Exception as e:
                    print(f"Unexpected error deleting {f}: {e}")

            print(f"Deleted {deleted_count} of {len(dwn_files)} PDF files.")
        except Exception as e:
            print(f"Failed to scan download folder: {e}")

    def summarize_downloads(self, number_of_files: int = 10) -> list[dict[str, str]]:
        """
        Summarize the download results for the first `number_of_files` entries in the DataFrame.

        This method checks the download status of the first `number_of_files` entries in the DataFrame.
        It determines whether each file was successfully downloaded or if there was an error.
        It also identifies which URL column (main or secondary) was used for the download attempt.

        Args: 
            number_of_files (int): The number of files to summarize.

        Returns:
            List(Dict): A summary list containing the ID, status, used column, and any error message for each file.

        Raises:
            AttributeError: If the DataFrame is not loaded.
            FileNotFoundError: If the download folder does not exist.
            KeyError: If expected columns are missing in the DataFrame.
            Exception: For any other unexpected errors.
        """
        try:
            if not hasattr(self, 'df2') or self.df2 is None:
                raise AttributeError("DataFrame 'df2' is not loaded. Please run load_excel() first.")

            if not self.dwn_folder.exists():
                raise FileNotFoundError(f"Download folder not found: {self.dwn_folder}")
            
            summary = []
            count_main_col = 0
            count_secondary_col = 0
            count_failed = 0

            for idx, row in self.df2.iloc[:number_of_files].iterrows(): # Iterate over the first `number_of_files` rows
                file_path = self.dwn_folder / f"{idx}.pdf"
                error = row.get("error", "")
                failed_main = "Failed to download" in str(error)

                if file_path.exists(): # if file exists
                    status = "Success"
                    used_col = self.secondary_col if failed_main else self.main_col
                    error = ""

                    if used_col == self.main_col: 
                        count_main_col += 1 # if main column was used
                    else:
                        count_secondary_col += 1 # if secondary column was used
                else:
                    status = "Failed"
                    used_col = self.secondary_col if failed_main else self.main_col
                    count_failed += 1 # if download failed

                summary.append({
                    "ID": idx,
                    "Status": status,
                    "UsedColumn": used_col,
                    "Error": error
                })

            print("\nDownload Summary:")
            for item in summary:
                print(f"ID: {item['ID']}, Status: {item['Status']}, Used: {item['UsedColumn']}, Error: {item['Error']}")

            print(f"Main Col Count: {count_main_col}, Secondary Col Count: {count_secondary_col}, Failed Count: {count_failed}")

        except AttributeError as ae:
            print(f"AttributeError: {ae}")
        except FileNotFoundError as fnf:
            print(f"FileNotFoundError: {fnf}")
        except KeyError as ke:
            print(f"Missing expected column in DataFrame: {ke}")
        except Exception as e:
            print(f"Unexpected error in summarize_downloads: {e}")

        return summary


if __name__ == "__main__":
    list_pth = r"C:\Users\SPAC-O-5\source\repos\PDFDownloader\GRI_2017_2020 (1).xlsx"
    output_folder = r"C:\Users\SPAC-O-5\source\repos\PDFDownloader\Output"
    main_col = "Pdf_URL"
    secondary_col = "Report Html Address"
    number_of_files = 20

    # Writer for reports
    report_writer = ReportWriter()
    report_writer.clean_report_file(output_folder=output_folder) # Clean report file at start

    # Prepare data
    prepare = PreparePdfDownloader(list_pth, output_folder, main_col, secondary_col)
    exist = prepare.prepare_folders_and_find_pdf_duplicates()
    df, df2 = prepare.load_and_filter_excel_data(exist)

    # Pass prepared data to downloader
    downloader = PDFDownloader(list_pth, output_folder, main_col, secondary_col)
    downloader.df = df
    downloader.df2 = df2

    downloader.delete_downloaded_files()
    downloader.process_downloads_threaded(number_of_files, max_workers=8)
    downloader.summarize_downloads(number_of_files)