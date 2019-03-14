import json
import logging
from utils import request


class FileCleanup:
    def __init__(self, service_url, client_name, json_data):
        self.__service_url = service_url
        self.__client_name = client_name
        self.__json_data = json_data

        logging.basicConfig(filename="report.log", filemode="a", level=logging.INFO)

    def cleanup(self):
        print("Complete server cleanup")
        file_list = json.loads(request(self.__json_data, 'list', self.__service_url, self.__client_name, {}))['List']
        for file in file_list:
            request(self.__json_data, 'remove', self.__service_url, self.__client_name, {'Hash': file['Hash']})
            logging.info("Removed " + file['Hash'] + " from " + self.__service_url)
