from utils import read_json
from file_submitter import FileSubmitter
from file_processor import FileProcessor
from file_cleanup import FileCleanup
import argparse
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("json_format_file")
    parser.add_argument("json_params_file")
    parser.add_argument("-u", "--upload", dest="u", action="store_true")
    parser.add_argument("-p", "--process", dest="p", action="store_true")
    parser.add_argument("-c", "--cleanup", dest="c", action="store_true")
    args = parser.parse_args()

    json_format = read_json(args.json_format_file)
    json_params = read_json(args.json_params_file)
    if json_format is None or json_params is None:
        exit(1)

    json_params['files_dir_path'] = os.path.join(os.getcwd(), json_params['files_dir_path'])
    json_params['downloads_dir_path'] = os.path.join(os.getcwd(), json_params['downloads_dir_path'])

    if not os.path.exists(json_params['files_dir_path']):
        os.mkdir(json_params['files_dir_path'])

    if not os.path.exists(json_params['downloads_dir_path']):
        os.mkdir(json_params['downloads_dir_path'])

    if args.u:
        fu = FileSubmitter(json_params['files_dir_path'], json_params['min_size'], json_params['min_size'],
                           json_params['number_of_processes'], json_params['service_url'], json_params['client_name'],
                           json_format, json_params['dump_list'], json_params['cleanup'])
        fu.start()
    elif args.p:
        fp = FileProcessor(json_params['service_url'], json_params['client_name'], json_params['downloads_dir_path'],
                           json_format)
        fp.process_files()
    elif args.c:
        fc = FileCleanup(json_params['service_url'], json_params['client_name'], json_format)
        fc.cleanup()
    else:
        print("No action given as parameter")
