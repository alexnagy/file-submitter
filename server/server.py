from flask import Flask, request, send_from_directory, url_for
from db_utils import *
import base64
import hashlib
import json
import os
import re


app = Flask(__name__)


class ServerOperations:
    def __init__(self):
        self.operation_count = {'add': 0, 'remove': 0, 'edit': 0, 'list': 0, 'details': 0, 'stats': 0}

    def add_file(self, client_id, parameters):
        try:
            if self.is_matching(parameters['Content'], parameters['Hash']) is False:
                return bad_request(1, 'Hash does not match content')

            size = len(self.decode_base64(parameters['Content']))

            metadata = None
            if 'Metadata' in parameters:
                metadata = json.dumps(parameters['Metadata'])

            self.add_file_to_db(parameters['Hash'], parameters['Content'], size, metadata, client_id)

            self.create_file_and_endpoint(client_id, parameters['Hash'], parameters['Content'])

            self.operation_count['add'] += 1

            return ok200('File submitted and stored successfully')
        except KeyError as e:
            return key_error_response(e)

    @staticmethod
    def add_file_to_db(hash, content, size, metadata, client_id):
        conn = create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO FILES (HASH, CONTENT, SIZE, METADATA, CLIENT_ID) VALUES (%s, %s, %s, %s, %s)",
                               [hash, content, size, metadata, client_id])
            conn.commit()
        except pymysql.IntegrityError:
            return bad_request(1, 'File already exists')
        finally:
            conn.close()

    def is_matching(self, content, hash):
        decoded_content = self.decode_base64(content)
        hashed_content = self.compute_md5hash(decoded_content)
        if hashed_content != hash:
            return False
        return True

    @staticmethod
    def decode_base64(data):
        return base64.b64decode(data)

    @staticmethod
    def compute_md5hash(data):
        return hashlib.md5(data).hexdigest()

    def create_file_and_endpoint(self, client_id, hash, content):
        temp_path = os.path.join(os.getcwd(), 'temp')
        if not os.path.exists(temp_path):
            os.mkdir(temp_path)

        path = os.path.join(os.getcwd(), 'temp', str(client_id))
        if not os.path.exists(path):
            os.mkdir(path)

        filename = os.path.join(path, hash)
        # non binary files
        try:
            f = open(filename, 'w+')
            # decode_base64 returns bytes so it must be decoded to utf-8
            f.write(self.decode_base64(content).decode('utf-8'))
            f.close()
        # binary files
        except UnicodeDecodeError:
            f = open(filename, 'wb')
            f.write(self.decode_base64(content))
            f.close()
        except Exception as e:
            print('The file with hash %s does not have an base64 encoded content and will be discarded. Exception:\n%s'
                  % (hash, e))
            if os.path.exists(filename):
                os.remove(filename)
            return

        download(client_id, hash)

    def remove_file(self, client_id, parameters):
        try:
            if self.file_exists_in_db(parameters['Hash'], client_id) is False:
                return bad_request(1, 'File not found')

            self.remove_file_from_db(parameters['Hash'], client_id)

            self.operation_count['remove'] += 1

            return ok200('File removed successfully')
        except KeyError as e:
            return key_error_response(e)

    @staticmethod
    def remove_file_from_db(hash, client_id):
        conn = create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM FILES WHERE HASH = %s AND CLIENT_ID = %s;", [hash, client_id])
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def file_exists_in_db(hash, client_id):
        conn = create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM FILES WHERE HASH = %s AND CLIENT_ID = %s;", [hash, client_id])
                if len(cursor.fetchall()) is 0:
                    return False
            return True
        finally:
            conn.close()

    def edit_file(self, client_id, parameters):
        try:
            if not self.file_exists_in_db(parameters['Hash'], client_id):
                return bad_request(1, 'File not found')

            metadata = json.dumps(parameters['Metadata'])

            self.edit_file_in_db(metadata, parameters['Hash'], client_id)

            self.operation_count['edit'] += 1

            return ok200('File edited successfully')
        except KeyError as e:
            return key_error_response(e)

    @staticmethod
    def edit_file_in_db(metadata, hash, client_id):
        conn = create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE FILES SET METADATA = %s WHERE HASH = %s AND CLIENT_ID = %s;",
                               [metadata, hash, client_id])
            conn.commit()
        finally:
            conn.close()

    def get_file_details(self, client_id, parameters):
        try:
            if self.file_exists_in_db(parameters['Hash'], client_id) is False:
                bad_request(1, 'File not found')

            content, size, metadata = self.get_file_details_from_db(parameters['Hash'], client_id)

            data = {'Status': 0, 'Message': 'File found',
                    'DownloadUrl': request.url.split('anagy/')[0] + url_for('download', client_id=client_id,
                                                                            filename=parameters['Hash'])[1:]
                    , 'Size': size}

            if metadata is not None:
                data['Metadata'] = metadata

            jsonData = json.dumps(data, indent=4)

            self.operation_count['details'] += 1

            return app.response_class(jsonData, mimetype='text/plain', status=200)
        except KeyError as e:
            return key_error_response(e)

    @staticmethod
    def get_file_details_from_db(hash, client_id):
        conn = create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT CONTENT, SIZE, METADATA FROM FILES WHERE HASH = %s AND CLIENT_ID = %s;",
                               [hash, client_id])
                elements = cursor.fetchall()
                return elements[0]
        finally:
            conn.close()

    def list_files(self, client_id, parameters):
        try:
            query = self.set_query_and_args(parameters)

            files = self.get_files_by_client_from_db(query, client_id)

            data = {'Status': 0, 'Message': 'Files listed successfully', 'Count': len(files), 'List': []}

            if 'Details' not in parameters or parameters['Details'] is False:
                data = self.list_without_details(files, data)
            elif parameters['Details'] is True:
                data = self.list_with_details(files, data, client_id)

            jsonData = json.dumps(data, indent=4)

            self.operation_count['list'] += 1

            return app.response_class(jsonData, mimetype='text/plain', status=200)
        except pymysql.OperationalError:
            return bad_request(1, 'Invalid parameters')
        except KeyError as e:
            return key_error_response(e)

    @staticmethod
    def set_query_and_args(parameters):
        query = "SELECT HASH, CONTENT, SIZE, METADATA FROM FILES WHERE CLIENT_ID = %s"

        if 'Limit' in parameters.keys() and 'Sort' in parameters.keys():
            query += " ORDER BY HASH " + parameters['Sort'] + " LIMIT " + str(parameters['Limit'])
        elif 'Limit' in parameters.keys():
            query += " LIMIT " + str(parameters['Limit'])
        elif 'Sort' in parameters.keys():
            query += " ORDER BY HASH " + parameters['Sort']

        query += ";"

        return query

    @staticmethod
    def get_files_by_client_from_db(query, client_id):
        conn = create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, [client_id])
                elements = cursor.fetchall()
                return elements
        finally:
            conn.close()

    @staticmethod
    def list_without_details(files, data):
        for file_tuple in files:
            data['List'].append({'Hash': file_tuple[0]})
        return data

    @staticmethod
    def list_with_details(files, data, client_id):
        for file_tuple in files:
            to_append = {'Hash': file_tuple[0], 'Size': file_tuple[2],
                         'DownloadUrl': request.url.split('anagy/')[0] + url_for('download', client_id=client_id,
                                                                                 filename=file_tuple[0])[1:]}
            if file_tuple[3] is not None:
                to_append['Metadata'] = file_tuple[3]
            data['List'].append(to_append)
        return data

    def get_stats(self, client_id):
        number_of_files = self.get_total_number_of_files_by_client(client_id)

        data = {'Status': 0, 'Message': 'Statistics queried successfully', 'FileCount': len(number_of_files),
                'OperationCount': json.dumps(self.operation_count)}

        jsonData = json.dumps(data, indent=4)

        self.operation_count['stats'] += 1

        return app.response_class(jsonData, mimetype='text/plain', status=200)

    @staticmethod
    def get_total_number_of_files_by_client(client_id):
        conn = create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM FILES WHERE CLIENT_ID = %s;", [client_id])
                return cursor.fetchone()[0]
        finally:
            conn.close()

    @staticmethod
    def add_client_to_db(client):
        conn = create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT NAME FROM CLIENTS WHERE NAME = %s;", [client])
                if len(cursor.fetchall()) is 0:
                    conn.cursor().execute("INSERT INTO CLIENTS (NAME) VALUES (%s);", [client])
                conn.commit()
        finally:
            conn.close()

    def run(self, operation, client_name, parameters):
        operations_dict = {'add': self.add_file, 'remove': self.remove_file, 'edit': self.edit_file,
                           'details': self.get_file_details, 'list': self.list_files, 'stats': self.get_stats}

        client_id = self.get_client_id_from_db(client_name)

        if operation == 'stats':
            return operations_dict[operation](client_id)
        else:
            try:
                return operations_dict[operation](client_id, parameters)
            except KeyError:
                return bad_request(-1, 'Invalid operation')
            except Exception as e:
                error_message = 'An error occurred on server while trying to process the request'
                print(error_message + '\n Exception: %s' % e)
                return bad_request(-2, error_message)

    @staticmethod
    def get_client_id_from_db(client_name):
        conn = create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT ID FROM CLIENTS WHERE NAME = %s;", [client_name])
                client_id = cursor.fetchall()[0][0]
                return client_id
        finally:
            conn.close()


def ok200(message):
    data = {'Status': 0, 'Message': message}
    jsonData = json.dumps(data, indent=4)
    return app.response_class(jsonData, mimetype='text/plain', status=200)


def bad_request(status, message):
    data = {'Status': status, 'Message': message}
    jsonData = json.dumps(data, indent=4)
    return app.response_class(jsonData, mimetype='text/plain', status=400)


def key_error_response(exception):
    m = re.search("'([^']*)'", str(exception))
    key = m.group(1)
    data = {'Status': -1, 'Message': key + ' key is missing'}
    jsonData = json.dumps(data, indent=4)
    return app.response_class(jsonData, mimetype='text/plain', status=400)


@app.route('/anagy/temp/<path:client_id>/<path:filename>', methods=['GET'])
def download(client_id, filename):
    temp = os.path.join(app.root_path, 'temp', str(client_id))
    return send_from_directory(directory=temp, filename=filename)


@app.route('/anagy/', methods=['POST'])
def start():
    content = request.get_json(silent=True)

    if content is None:
        return bad_request(1, 'Invalid json format')

    try:
        operation = content['Operation']
        client = content['Client']
        parameters = content['Parameters']

        operations = ServerOperations()

        operations.add_client_to_db(client)

        response = operations.run(operation, client, parameters)

        return response
    except KeyError as e:
        key_error_response(e)


if __name__ == '__main__':
    if create_clients_table() is False:
        exit(1)
    if create_files_table() is False:
        exit(1)

    app.run(host='127.0.0.1', port=8080)
