import unittest
import xmlrunner
import pandas as pd
from unittest.mock import patch
from unittest.mock import mock_open
from download_files import PDFDownloader
from download_files import PreparePdfDownloader
from report_writer import ReportWriter
import requests

class test_prepare(unittest.TestCase):
    def setUp(self):
        self.list_pth = r"C:\Users\SPAC-O-1\Projekter\uge-4\data\GRI_2017_2020 - Kopi.xlsx"
        self.output_folder = r"C:\Users\SPAC-O-1\Projekter\uge-5\PDFDownloader\output"
        self.main_col = "Pdf_URL"
        self.secondary_col = "Report Html Address"
        self.number_of_files = 20
        self.downloader = PDFDownloader(self.list_pth, self.output_folder, self.main_col, self.secondary_col)
        self.prepare = PreparePdfDownloader(self.list_pth, self.output_folder, self.main_col, self.secondary_col)
        self.report_writer = ReportWriter()

    def tearDown(self):
        del self.downloader

    def test_run_prepare(self):
        exist = self.prepare.prepare_folders_and_find_pdf_duplicates()
        self.assertIsInstance(self.prepare.load_and_filter_excel_data(exist), (tuple))

    def test_pass(self):
        exist = self.prepare.prepare_folders_and_find_pdf_duplicates()
        df, df2 = self.prepare.load_and_filter_excel_data(exist)
        self.downloader.df = df
        self.downloader.df2 = df2
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)

    def test_download(self):
        self.assertEqual(self.downloader.delete_downloaded_files(), None)
        self.assertEqual(self.downloader.process_downloads_threaded(self.number_of_files, max_workers=8), None)
        self.assertEqual(self.downloader.summarize_downloads(self.number_of_files), (list(dict([str, str]))))

    def test_full_download(self):
        self.assertEqual(self.report_writer.clean_report_file(output_folder=self.output_folder), None) # Clean report file at start
        exist = self.prepare.prepare_folders_and_find_pdf_duplicates()
        df, df2 = self.prepare.load_and_filter_excel_data(exist)
        self.downloader.df = df
        self.downloader.df2 = df2
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(self.downloader.delete_downloaded_files(), None)
        self.assertEqual(self.downloader.process_downloads_threaded(self.number_of_files, max_workers=8), None)
        self.assertEqual(self.downloader.summarize_downloads(self.number_of_files), (list(dict([str, str]))))

if __name__ == '__main__':
    unittest.main()
