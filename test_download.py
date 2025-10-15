import unittest
from unittest.mock import patch
from download_files import PDFDownloader
import requests

class test_url_methods(unittest.TestCase):

    def setUp(self):
        mock_list_path = "C:/Users/SPAC-O-1/Projekter/uge-4/data/GRI_2017_2020 - Kopi.xlsx"
        mock_output_folder = r"C:\Users\SPAC-O-1\Projekter\uge-5\PDFDownloader\output"
        mock_main_col = "Pdf_URL"
        mock_secondary_col = "Report Html Address"
        self.downloader = PDFDownloader(mock_list_path, mock_output_folder, mock_main_col, mock_secondary_col)

    def tearDown(self):
        del self.downloader

    def test_download(self):

        mock_savepath = "C:/Users/SPAC-O-1/Projekter/uge-5/PDFDownloader/output"
        mock_url = "http://cdn12.a1.net/m/resources/media/pdf/A1-Umwelterkl-rung-2016-2017.pdf"
        self.assertEqual(self.downloader.download_pdf(mock_url, mock_savepath), [True, ""])

    def test_threading(self):
        self.assertEqual(self.downloader.process_downloads_threaded(10, 4), None)
    
    def test_deletion(self):
        self.assertEqual(self.downloader.delete_downloaded_files(), None)

if __name__ == "__main__":
    unittest.main()