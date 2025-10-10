import csv
import os

class ReportWriter:
    def write_to_report(self, name: str, result: str, output_folder: str, filename: str ="Download_result_report.csv", sep: str =";") -> None:
        """
        Write the download result to a report file.

        This method appends a row to a CSV file with the provided name and result. If the `clean` parameter is set to True,
        it will first clear the contents of the file before writing. The CSV file will have a header row if it is being created for the first time.

        Args:
            name (str): The name or identifier of the file being reported on.
            result (str): The result of the download attempt (e.g., "Downloaded", "Not downloaded").
            filename (str): The name of the CSV file to write the report to. Default is "Download_result_report.csv".
            sep (str): The separator to use in the CSV file. Default is ";".
            clean (bool): If True, clears the contents of the file before writing. Default is False.

        Returns:
            None

        Raises:
            OSError: If there are issues opening or writing to the file.
            IOError: If there are I/O issues during file operations.
            Exception: For any other unexpected errors.
        """
        try:
            if not os.path.exists(output_folder / filename):
                open(output_folder / filename, 'x', newline='', encoding='utf-8').close() # Create the file if it doesn't exist

            report_path = output_folder / filename
            with open(report_path, "a", newline="", encoding="utf-8") as csvfile: # Opens the file in append mode
                writer = csv.writer(csvfile, delimiter=sep)

                if(os.stat(report_path).st_size == 0): # if file is empty
                    writer.writerow(["Name", "Result"]) # Write header

                writer.writerow([name, result]) # Write the data
            print(f"Wrote report: {name} ; {result}")
        except (OSError, IOError) as file_error:
            print(f"File I/O error when writing report: {file_error}")
        except Exception as e:
            print(f"Unexpected error in write_to_report: {e}")
    
    def clean_report_file(self, output_folder: str, filename: str ="Download_result_report.csv") -> None:
        """
        Clear the contents of the report file.

        This method opens the specified CSV file and clears its contents. If the file does not exist, it will be created.

        Args:
            filename (str): The name of the CSV file to clear. Default is "Download_result_report.csv".

        Returns:
            None

        Raises:
            OSError: If there are issues opening or writing to the file.
            IOError: If there are I/O issues during file operations.
            Exception: For any other unexpected errors.
        """
        try:
            report_path = output_folder / filename
            with open(report_path, "w", newline="", encoding="utf-8"): # Opens the file in write mode, which clears it
                pass # Just open and close to clear the file

            print(f"Cleared report file: {report_path}")
        except (OSError, IOError) as file_error:
            print(f"File I/O error when clearing report file: {file_error}")
        except Exception as e:
            print(f"Unexpected error in clean_report_file: {e}")