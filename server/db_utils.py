import pymysql


def create_connection():
    try:
        connection = pymysql.connect(host='localhost', user='root', db='test')
        return connection
    except pymysql.Error as e:
        print(e)


def create_clients_table():
    conn = create_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                     CREATE TABLE IF NOT EXISTS CLIENTS
                     (ID      INTEGER           PRIMARY KEY     AUTO_INCREMENT,
                      NAME    VARCHAR(30)       UNIQUE
                     );
                     ''')
        return True
    except pymysql.OperationalError as e:
        print(e, 'in creating table CLIENTS')
        return False
    finally:
        conn.close()


def create_files_table():
    conn = create_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                      CREATE TABLE IF NOT EXISTS FILES
                     (HASH       CHAR(32)         NOT NULL,
                      CONTENT    VARCHAR(200)     NOT NULL,
                      SIZE       INTEGER          NOT NULL,
                      METADATA   VARCHAR(200),
                      CLIENT_ID  INTEGER,
                      PRIMARY KEY (HASH, CLIENT_ID),
                      FOREIGN KEY (CLIENT_ID) REFERENCES CLIENTS(ID)
                     );
                     ''')
        return True
    except pymysql.OperationalError as e:
        print(e, 'in creating table FILES')
        return False
    finally:
        conn.close()
