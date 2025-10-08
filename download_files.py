# -*- coding: utf-8 -*-
"""
Created on Sun Oct 13 15:37:08 2019

@author: hewi
"""

#### IF error : "ModuleNotFOundError: no module named PyPDF2"
   # then uncomment line below (i.e. remove the #):
       
#pip install PyPDF2

import pandas as pd
import requests
from pathlib import Path
import glob
import os
import time
import concurrent.futures

class PDFDownloader:
    def __init__(self, list_path, output_folder, main_col, secondary_col, id_col="BRnum"):
        self.list_path = list_path
        self.output_folder = Path(output_folder)
        self.dwn_folder = self.output_folder / "dwn"
        self.main_col = main_col
        self.secondary_col = secondary_col
        self.id_col = id_col
        self.df = None
        self.df2 = None

    def prepare(self):
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.dwn_folder.mkdir(parents=True, exist_ok=True)
        dwn_files = glob.glob(str(self.dwn_folder / "*.pdf"))
        exist = [os.path.basename(f)[:-4] for f in dwn_files]
        df = pd.read_excel(self.list_path, sheet_name=0, index_col=self.id_col)
        df = df[df[self.main_col].notnull() & df[self.secondary_col].notnull()]
        self.df = df
        self.df2 = df[~df.index.isin(exist)].copy()

    def download_pdf(self, url, savepath):
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(savepath, 'wb') as f:
                    f.write(response.content)
                return True, ""
            else:
                return False, f"Failed to download: {response.status_code} - {response.reason}"    
        except Exception as e:
            return False, f"Failed to download: {str(e)}"
        
    def process_downloads(self, number_of_files=10):
        start_time = time.perf_counter()
        for j in self.df2.index[:number_of_files]:
            savefile = self.dwn_folder / f"{j}.pdf"
            success, error = self.download_pdf(self.df2.at[j, self.main_col], savefile)
            if not success:
                self.df2.at[j, "error"] = error
                # Try secondary column
                success2, error2 = self.download_pdf(self.df2.at[j, self.secondary_col], savefile)
                if not success2:
                    self.df2.at[j, "error"] = error2
        end_time = time.perf_counter()
        print(f"Downloaded Single thread {min(number_of_files, len(self.df2))} files in {end_time - start_time:.2f} seconds.")

    def process_downloadsThreaded(self, number_of_files=10, max_workers=4): # test result vs single thread: 4.75 sec vs 16.64 sec for 10 files. with 4 workers
        start_time = time.perf_counter()
        def download_task(j):
            savefile = self.dwn_folder / f"{j}.pdf"
            success, error = self.download_pdf(self.df2.at[j, self.main_col], savefile)
            if not success:
                self.df2.at[j, "error"] = error
                # Try secondary column
                success2, error2 = self.download_pdf(self.df2.at[j, self.secondary_col], savefile)
                if not success2:
                    self.df2.at[j, "error"] = error2

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(download_task, self.df2.index[:number_of_files])
        end_time = time.perf_counter()
        print(f"Downloaded Threaded with {max_workers} workers {min(number_of_files, len(self.df2))} files in {end_time - start_time:.2f} seconds.")

    def delete_downloaded_files(self):
        dwn_files = glob.glob(str(self.dwn_folder / "*.pdf"))
        for f in dwn_files:
            os.remove(f)
        print(f"Deleted {len(dwn_files)} downloaded files.")

    def summarize_downloads(self, number_of_files=10):
        summary = []
        for idx, row in self.df2.iloc[:number_of_files].iterrows():
            file_path = self.dwn_folder / f"{idx}.pdf"
            used_col = None
            status = "Failed"
            error = row.get("error", "")
            if file_path.exists():
                status = "Success"
                used_col = self.secondary_col if "Failed to download" in str(error) else self.main_col
                error = ""
            else:
                used_col = self.secondary_col if "Failed to download" in str(error) else self.main_col
            summary.append({
                "ID": idx,
                "Status": status,
                "UsedColumn": used_col,
                "Error": error
            })
        print("\nDownload Summary:")
        for item in summary:
            print(f"ID: {item['ID']}, Status: {item['Status']}, Used: {item['UsedColumn']}, Error: {item['Error']}")
        return summary

if __name__ == "__main__":
    list_pth = r"C:\Users\SPAC-O-5\source\repos\PDFDownloader\GRI_2017_2020 (1).xlsx"
    output_folder = r"C:\Users\SPAC-O-5\source\repos\PDFDownloader\Output"
    main_col = "Pdf_URL"
    secondary_col = "Report Html Address"

    downloader = PDFDownloader(list_pth, output_folder, main_col, secondary_col)
    downloader.prepare()
    downloader.process_downloads(number_of_files=10)
    # downloader.process_downloadsThreaded(number_of_files=10)
    # downloader.summarize_downloads(number_of_files=10)
    downloader.delete_downloaded_files()
    downloader.process_downloadsThreaded(number_of_files=10)
