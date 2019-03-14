import threading
import signal
import os
import logging
import time
import multiprocessing
import multiprocessing.util
import multiprocessing.managers
from utils import *


class FileSubmitter:
    def __init__(self, files_dir_path, min_file_size, max_file_size, processes_number, service_url, client_name,
                 json_data, dump_list=False, cleanup=False):

        # Input parameters
        self.files_dir_path = files_dir_path
        self.min_file_size = min_file_size
        self.max_file_size = max_file_size
        self.processes_number = processes_number
        self.service_url = service_url
        self.client_name = client_name
        self.json_data = json_data
        self.dump_list = dump_list
        self.cleanup = cleanup

        self.threads_list = []

        # Creating shared data structures with multiprocessing.Manager()
        manager = multiprocessing.Manager()
        self.generated_files = manager.list()
        self.errors = manager.list()
        self.running_processes = manager.Value('i', 0)
        self.should_run = manager.Event()

        # Logger setup and a shared queue for logs by all processes
        logging.basicConfig(filename="report.log", filemode="w", level=logging.INFO)
        self.logs = multiprocessing.Queue()

        self.threads_number = None

        self.is_log_active = True

    def estimate_threads_number(self):
        upload_time_list = []

        self.is_log_active = False

        for i in range(10 * self.processes_number):
            start = time.time()
            path = self.generate_file()
            if path is not None:
                self.upload_file(path)
            end = time.time()

            upload_time_list.append(int(self.processes_number // (end - start)))

        self.dir_cleanup(self.files_dir_path, self.is_log_active)

        for el in self.generated_files:
            self.generated_files.remove(el)

        self.is_log_active = True

        return int(sum(upload_time_list) // len(upload_time_list))

    def start(self):
        print("Do not press any key!")

        self.threads_number = self.estimate_threads_number()

        print("Starting %s processes, %s threads" % (self.processes_number, self.threads_number))

        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)

        pool = multiprocessing.Pool(processes=self.processes_number, initializer=self.worker)

        signal.signal(signal.SIGINT, original_sigint_handler)

        try:
            while self.running_processes.value != self.processes_number:
                pass
            time.sleep(5)
            print("Press Ctrl+C to exit")
            while True:
                pass
        except KeyboardInterrupt:
            self.log_to_file()
            pool.terminate()
            pool.join()
            print("Keyboard interrupt")

        created = []

        for el in self.generated_files:
            created.append(el)

        if self.dump_list:
            print("Logging files and hashes")
            self.log_path_and_hash(created)

        if self.cleanup:
            print("Cleanup")
            self.dir_cleanup(self.files_dir_path, self.is_log_active)

    def log_to_file(self):
        while not self.logs.empty():
            error_level, message = self.logs.get()
            if error_level == 'info':
                logging.info(message)
            elif error_level == 'warning':
                logging.warning(message)
            elif error_level == 'error':
                logging.error(message)

    def worker(self):
        self.running_processes.value += 1
        for index in range(self.threads_number):
            thread = threading.Thread(target=self.generate_and_upload_files)
            self.threads_list.append(thread)

        for thread in self.threads_list:
            if not thread.is_alive():
                thread.start()

        try:
            while True:
                pass
        except KeyboardInterrupt:
            self.logs.cancel_join_thread()
            for thread in self.threads_list:
                thread.join()

    def generate_and_upload_files(self):
        while True:
            file_path = self.generate_file()
            if file_path is not None:
                self.upload_file(file_path)
            time.sleep(0.1)

    def generate_file(self):
        size = random.randint(self.min_file_size, self.max_file_size)
        data = os.urandom(size)
        data_hash = md5hash(data)
        if data_hash not in self.generated_files:
            file_name = generate_random_string() + ".bin"
            file_path = self.files_dir_path + "\\" + file_name
            f = open(file_path, 'wb')
            f.write(data)
            f.close()
            if self.is_log_active:
                self.generated_files.append(data_hash)
                self.logs.put(['info', "Generated file " + file_name + " in " + self.files_dir_path])
            return file_path
        else:
            if self.is_log_active:
                self.logs.put(['warning', "A file with hash " + data_hash + "was already generated"])
        return None

    def upload_file(self, file_path):
        f = open(file_path, 'rb')
        data = f.read()
        f.close()
        data_hash = md5hash(data)
        parameters = {"Hash": data_hash, "Content": b64encode(data), "Metadata": {'Processed': False}}

        try:
            request(self.json_data, 'add', self.service_url, self.client_name, parameters)
            if self.is_log_active:
                self.logs.put(['info', "Uploaded file " + file_path.split("\\")[-1] + " to " + self.service_url])
        except urllib.error.URLError as error:
            self.errors.append(error)
            os.remove(file_path)
            if self.is_log_active:
                self.logs.put(['error', "Server error."])
                self.logs.put(['info', "Removed " + file_path.split("\\")[-1] + " from " + self.files_dir_path])

    def log_path_and_hash(self, hashes):
        log_file_path = os.path.join(os.getcwd(), "files.log")
        log = open(log_file_path, 'w')
        for file in os.listdir(self.files_dir_path):
            file_path = os.path.join(self.files_dir_path, file)
            f = open(file_path, 'rb')
            data = f.read()
            f.close()
            fileHash = md5hash(data)
            if fileHash in hashes:
                log.write("Path: " + file_path + "\n")
                log.write("Hash: " + md5hash(data) + "\n\n")
        log.close()

    @staticmethod
    def dir_cleanup(dir_path, is_log_active):
        for file in os.listdir(dir_path):
            file_path = os.path.join(dir_path + "\\" + file)
            os.remove(file_path)
            if is_log_active:
                logging.info("Removed file " + file + " from " + dir_path)
