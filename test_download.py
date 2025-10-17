import unittest
import xmlrunner
from unittest.mock import patch
from unittest.mock import mock_open
from download_files import PDFDownloader
from download_files import PreparePdfDownloader
from report_writer import ReportWriter
import requests

class test_prepare_pdf(unittest.TestCase):

    def setUp(self):
        mock_list_path = "C:/Users/SPAC-O-1/Projekter/uge-4/data/GRI_2017_2020 - Kopi.xlsx"
        mock_output_folder = r"C:\Users\SPAC-O-1\Projekter\uge-5\PDFDownloader\output"
        mock_main_col = "Pdf_URL"
        mock_secondary_col = "Report Html Address"
        self.prepare_pdf = PreparePdfDownloader(mock_list_path, mock_output_folder, mock_main_col, mock_secondary_col)

    def tearDown(self):
        del self.prepare_pdf

    def test_prepare_and_find(self):
        with patch("glob.glob", return_value = [], side_effect=[PermissionError, OSError, Exception]):
            self.assertIsInstance(self.prepare_pdf.prepare_folders_and_find_pdf_duplicates(), (list)) 

    def test_load_and_filter(self):
        exist = self.prepare_pdf.prepare_folders_and_find_pdf_duplicates()
        with patch("glob.glob", return_value = [], side_effect=[PermissionError, OSError, Exception]):
            self.assertIsInstance(self.prepare_pdf.load_and_filter_excel_data(exist), (tuple)) 

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
        response_success = requests.Response()
        response_success.status_code = 200
        with patch("requests.get", return_value = response_success, side_effect = [response_success, requests.exceptions.HTTPError, requests.exceptions.ConnectionError, requests.exceptions.MissingSchema]) as mock_download:
            self.assertEqual(self.downloader.download_pdf(mock_url, mock_savepath), (True, ''))

    def test_threading(self):
        #self.assertEqual(self.downloader.process_downloads_threaded(10, 4), None)
        response_success = requests.Response()
        response_success.status_code = 200
        response_success.encoding = 'pdf'
        with patch("requests.get", return_value = response_success, side_effect = [response_success, requests.exceptions.HTTPError, requests.exceptions.ConnectionError, requests.exceptions.MissingSchema]) as mock_threading:
            self.downloader.process_downloads_threaded(10,4)
            with self.assertRaises(tuple([requests.exceptions.HTTPError, requests.exceptions.ConnectionError, requests.exceptions.MissingSchema])):
                self.downloader.process_downloads_threaded(10,4)
    
    def test_deletion(self):
        with patch("glob.glob", return_value = ["BR50050", "Downloaded"], side_effect = [FileNotFoundError, PermissionError, Exception]) as mock_deletion:
            self.assertEqual(self.downloader.delete_downloaded_files(), None)
    
    def test_summary(self):
        self.assertIsInstance(self.downloader.summarize_downloads(), (list(dict(str, str))))

class test_report_writer(unittest.TestCase):
    def setUp(self):
        self.report_writer = ReportWriter()
    
    def tearDown(self):
        del self.report_writer

    def test_clean_report(self):
        mock_output_folder = "C:/Users/SPAC-O-1/Projekter/uge-5/PDFDownloader/output"
        with patch("builtins.open", new_callable=mock_open) as m:
            self.assertEqual(self.report_writer.clean_report_file(output_folder=mock_output_folder), None)
        
    def test_write_report(self):
        mock_output_folder = "C:/Users/SPAC-O-1/Projekter/uge-5/PDFDownloader/output"
        mock_name = f"BR50042"
        mock_result = f"Downloaded"
        with patch("builtins.open", new_callable=mock_open) as m:
            self.assertEqual(self.report_writer.write_to_report(mock_name, mock_result, mock_output_folder), None)
    
if __name__ == "__main__":
    with open("test_results.txt", "w") as f:
        # Redirect the output to the file
        runner = unittest.TextTestRunner(stream=f, verbosity=2)
        unittest.main(testRunner=runner, exit=False)