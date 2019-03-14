import json
import urllib.request
import string
import base64
import hashlib
import random
import http.client
from urllib.error import URLError
from json import JSONDecodeError


def read_json(json_path):
    try:
        json_file_handle = open(json_path, "r+")
        data = json.load(json_file_handle)
        json_file_handle.close()
        return data
    except JSONDecodeError:
        print("Invalid JSON")
        return None


def request(json_data, operation, service_url, client_name, parameters):
    try:
        is_json_valid(json_data)
        is_operation_valid(operation)
        is_parameters_valid(parameters)

        json_data["Operation"] = operation
        json_data["Client"] = client_name
        json_data["Parameters"] = parameters

        req = urllib.request.Request(service_url)
        req.add_header('Content-Type', 'application/json')

        with urllib.request.urlopen(req, json.dumps(json_data).encode('utf8')) as f:
            return f.read().decode('utf8')
    except ValueError:
        print("Invalid JSON")
    except TypeError:
        print("Invalid operation or parameters")
    except URLError:
        print("Server request failed")
    except http.client.RemoteDisconnected:
        request(json_data, operation, service_url, client_name, parameters)


def is_json_valid(data):
    keys = data.keys()
    if "Operation" not in keys or "Client" not in keys or "Parameters" not in keys:
        raise ValueError


def is_operation_valid(operation):
    if operation not in ["add", "remove", "edit", "details", "list", "stats"]:
        raise TypeError


def is_parameters_valid(parameters):
    if type(parameters) is not dict:
        raise TypeError


def md5hash(data):
    return hashlib.md5(data).hexdigest()


def b64encode(data):
    return base64.b64encode(data).decode('utf8')


def generate_random_string():
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(random.randint(10, 20)))
