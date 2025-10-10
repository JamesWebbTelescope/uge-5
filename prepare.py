
import pandas as pd
import glob
import os
from pathlib import Path

class PreparePdfDownloader:
    def __init__(self, list_path, output_folder, main_col, secondary_col, id_col="BRnum"):
        self.list_path = list_path
        self.output_folder = Path(output_folder)
        self.dwn_folder = self.output_folder / "dwn"
        self.main_col = main_col
        self.secondary_col = secondary_col
        self.id_col = id_col

    def prepare_folders_and_find_pdf_duplicates(self):
        """
        Create output and download folders if they don't exist.
        Return a list of existing PDF filenames (without extension) in the download folder.

        Returns:
            List[str]: A list of existing PDF filenames (without the .pdf extension).

        Raises:
            PermissionError: If there are permission issues creating the folders.
            OSError: For other OS-related errors during folder creation.
            Exception: For any other unexpected errors.
        """
        try:
            self.output_folder.mkdir(parents=True, exist_ok=True)
            self.dwn_folder.mkdir(parents=True, exist_ok=True)

            self.dwn_files = glob.glob(str(self.dwn_folder / "*.pdf"))
            exist = [os.path.basename(f)[:-4] for f in self.dwn_files]

            return exist

        except PermissionError as pe:
            print(f"Error: Permission denied when creating folders - {pe}")
        except OSError as ose:
            print(f"Error: OS error during folder creation - {ose}")
        except Exception as e:
            print(f"Unexpected error in prepare_folders_and_list_downloaded_files(): {e}")
        
        return []

    def load_and_filter_excel_data(self, exist:list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load the Excel file into a DataFrame, validate required columns,
        filter out rows with missing values, and filter out already downloaded entries.

        Args:
            exist (list[str]): List of existing PDF filenames (without extension) to filter out.

        Raises:
            FileNotFoundError: If the Excel file does not exist.
            pd.errors.EmptyDataError: If the Excel file is empty or corrupt.
            KeyError: If required columns are missing in the DataFrame.
            PermissionError: If there are permission issues reading the file.
            ValueError: For invalid parameters when reading the Excel file.
            Exception: For any other unexpected errors.
        """
        try:
            if not os.path.isfile(self.list_path):
                raise FileNotFoundError(f"The Excel file does not exist at: {self.list_path}")

            df = pd.read_excel(self.list_path, sheet_name=0, index_col=self.id_col)

            required_cols = [self.main_col, self.secondary_col]
            for col in required_cols:
                if col not in df.columns:
                    raise KeyError(f"Missing required column: {col}")

            # Filter rows with valid data
            df = df[df[self.main_col].notnull() & df[self.secondary_col].notnull()]

            self.df = df
            self.df2 = df[~df.index.isin(exist)].copy()

            return self.df, self.df2
        except FileNotFoundError as fnf_error:
            print(f"Error: Excel file not found - {fnf_error}")
        except pd.errors.EmptyDataError as ede:
            print(f"Error: The Excel file is empty or corrupt - {ede}")
        except KeyError as ke:
            print(f"Error: Missing expected column in Excel - {ke}")
        except PermissionError as pe:
            print(f"Error: Permission denied when reading the Excel file - {pe}")
        except ValueError as ve:
            print(f"Error: Invalid parameters when reading Excel - {ve}")
        except Exception as e:
            print(f"Unexpected error in load_and_filter_excel_data(): {e}")
