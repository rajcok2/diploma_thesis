import psycopg2
from source.Constants import *


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Database(metaclass=Singleton):
    def __init__(self):
        self.isConnected = False
        self.connection = None
        self.cursor = None

    def connect(self):
        host = DB_HOST
        port = DB_PORT
        database_name = DB_NAME
        name = DB_USER
        password = DB_PASSWORD

        try:
            self.connection = psycopg2.connect(host=host,
                                               port=port,
                                               dbname=database_name,
                                               user=name,
                                               password=password)
            self.cursor = self.connection.cursor()
            self.connection.autocommit = True
            self.cursor.execute("rollback")
            self.isConnected = True

            print('connected')

            return self.isConnected

        except psycopg2.OperationalError as error:
            return str(error)

    def disconnect(self):
        if self.isConnected:
            self.cursor.close()
            self.isConnected = False

    def set_stat_statements_extension(self):
        extension = "pg_stat_statements"
        self.execute_command(f"CREATE EXTENSION IF NOT EXISTS {extension};")
        self.execute_command("SELECT pg_stat_statements_reset();")

    def get_stat_statements(self):
        rec = self.execute_command("SELECT query, calls, total_time, rows, 100.0 * shared_blks_hit/"
                                    "nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent "
                                    "FROM pg_stat_statements "
                                    "ORDER BY calls")

        return rec

    def execute_command(self, data):
        try:
            self.cursor.execute(data)
        except psycopg2.Error as error:
            print("chybny command")
            print(data)
            return error
        try:
            records = self.cursor.fetchall()
            return self.format_records(records)
        except psycopg2.Error as error:
            print(error)
            print('chybaaaaaaaa v execute_command druhy except')
            pass

    def format_records(self, records):
        table_header = []
        for i in range(len(self.cursor.description)):
            table_header.append(self.cursor.description[i][0])

        table_rows_data = []
        row_data = []
        for i in range(len(records)):
            temp = (records[i])
            for j in range(len(temp)):
                row_data.append(str(temp[j]))
            table_rows_data.append(row_data)
            row_data = []

        return table_rows_data


class Metadata:
    def __init__(self):
        self.db_conn = Database()
        self.columns = dict()

    def get_metadata(self, tables):
        tables_and_columns = dict()
        self.db_conn.cursor.execute("SELECT DISTINCT "
                                    "    column_name, table_name\n "
                                    "FROM "
                                    "    information_schema.columns "
                                    "WHERE "
                                    "    table_schema = 'my_schema' "
                                    "ORDER BY "
                                    "    table_name")
        for data in self.db_conn.cursor.fetchall():
            table_name = data[1]
            column_name = data[0]
            if table_name in tables:
                if table_name not in tables_and_columns:
                    tables_and_columns[table_name] = set()
                tables_and_columns[table_name].add(column_name)

        return tables_and_columns


if __name__ == '__main__':
    db = Database()
    db.connect()
    m = Metadata()
    m.get_metadata()
