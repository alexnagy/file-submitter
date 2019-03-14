import urllib.request
import urllib.error
import os
import logging
from utils import *


class FileProcessor:
    def __init__(self, service_url, client_name, download_dir_path, json_data):
        self.__service_url = service_url
        self.__client_name = client_name
        self.__download_dir_path = download_dir_path
        self.__json_data = json_data

        self.__files_hashes = []

        logging.basicConfig(filename="report.log", filemode="a", level=logging.INFO)

    def process_files(self):
        print("Downloading files")
        self.download_files()
        print("Computing size of files")
        self.calculate_size_of_files()
        print("Uploading files")
        self.upload_files()
        print("Directory cleanup")
        self.dir_cleanup(self.__download_dir_path)

    def download_files(self):
        file_list = json.loads(request(self.__json_data, 'list', self.__service_url, self.__client_name,
                                       {'Details': True}))['List']
        for file in file_list:
            self.__files_hashes.append(file['Hash'])
            metadata = json.loads(file['Metadata'])
            if metadata['Processed'] is False:
                self.download_file(file['DownloadUrl'])

    def download_file(self, download_url):
        file_name = generate_random_string() + ".bin"
        try:
            urllib.request.urlretrieve(url=download_url, filename=self.__download_dir_path + "\\" + file_name)
            logging.info("Downloaded " + file_name + " to " + self.__download_dir_path)
        except urllib.error.HTTPError as e:
            logging.error(e)

    def calculate_size_of_files(self):
        for file in os.listdir(self.__download_dir_path):
            os.path.getsize(self.__download_dir_path + "\\" + file)

    def server_cleanup(self):
        file_list = json.loads(request(self.__json_data, 'list', self.__service_url, self.__client_name, {}))['List']
        for file in file_list:
            if file['Hash'] in self.__files_hashes:
                request(self.__json_data, 'remove', self.__service_url, self.__client_name, {'Hash': file['Hash']})
                logging.info("Removed " + file['Hash'] + " from " + self.__service_url)

    def upload_files(self):
        for file in os.listdir(self.__download_dir_path):
            self.upload_file(self.__download_dir_path + "\\" + file)

    def upload_file(self, file_path):
        f = open(file_path, 'rb')
        data = f.read()
        f.close()
        data_hash = md5hash(data)
        parameters = {"Hash": data_hash, "Metadata": {'Processed': True}}

        try:
            request(self.__json_data, 'edit', self.__service_url, self.__client_name, parameters)
            logging.info("Uploaded file " + file_path.split("\\")[-1] + " to " + self.__service_url)
        except URLError:
            os.remove(file_path)
            logging.error("Server error.")
            logging.info("Removed " + file_path.split("\\")[-1] + " from " + self.__download_dir_path)

    @staticmethod
    def dir_cleanup(dir_path):
        for file in os.listdir(dir_path):
            os.remove(dir_path + "\\" + file)
            logging.info("Removed file " + file + " from " + dir_path)
