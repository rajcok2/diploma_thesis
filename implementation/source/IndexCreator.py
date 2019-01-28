from source.DatabaseManager import Database
from itertools import permutations, product


class Index:
    def __init__(self, name, schema, table, columns):
        self.name = name
        self.schema = schema
        self.table = table
        self.columns = columns


class IndexCreator:
    def __init__(self, query, schema, tables_and_columns):
        self.tables_and_columns = tables_and_columns
        self.schema = schema
        self.main_query = "select e.first_name from my_schema.my_table_jobs j, my_schema.my_table_employees e " \
           "where e.job_id = j.job_id and e.salary > j.min_salary;"
        self.db_conn = Database()
        self.indexes = dict()
        self.index_combinations = list()

    def create_combination(self):
        print("...........")
        for table, value in self.tables_and_columns.items():
            list(value).sort()
            ordered_columns = value
            for number in range(len(ordered_columns) + 1):
                combined_columns = permutations(ordered_columns, number)
                for columns in combined_columns:
                    if columns:
                        columns_string = '_'.join(columns)
                        index_name = table + '_' + columns_string + '_index'
                        index = Index(index_name, self.schema, table, columns)
                        if table not in self.indexes:
                            self.indexes[table] = list()
                        self.indexes[table].append(index)

        self.create_indexes()

    def create_indexes(self):
        indexes = self.indexes.values()
        self.index_combinations = product(*indexes)
        print("tabulky a stlpce", self.tables_and_columns)
        print("vygenerovane indexy", self.indexes)
        print("////////////////////kombinacie indexov")
        for n_tuple in self.index_combinations:
            print("*****")
            self.set_indexes(n_tuple)
            for obj_index in n_tuple:
                print(obj_index.name)

    def set_indexes(self, indexes):
        self.db_conn.cursor.execute(f'EXPLAIN (ANALYZE true, COSTS true, FORMAT json) {self.main_query}')
        output = self.db_conn.cursor.fetchall()
        print(output[0][0][0]['Execution Time'])

        for index in indexes:
            if index.schema:
                index_name = '.'.join([index.schema, index.name])
                table_name = '.'.join([index.schema, index.table])
            else:
                index_name = index.name
                table_name = index.table

            self.db_conn.cursor.execute(f'drop index if exists {index_name}')
            print(f"create index {index.name} on {table_name} ({', '.join(index.columns)})")
            self.db_conn.cursor.execute(f"create index {index.name} on {table_name} ({', '.join(index.columns)})")

        self.db_conn.cursor.execute(f'EXPLAIN (ANALYZE true, COSTS true, FORMAT json) {self.main_query}')
        output = self.db_conn.cursor.fetchall()
        print('...with index', output[0][0][0]['Execution Time'])

        for index in indexes:
            if index.schema:
                index_name = '.'.join([index.schema, index.name])
                table_name = '.'.join([index.schema, index.table])
            else:
                index_name = index.name
                table_name = index.table

            self.db_conn.cursor.execute(f'drop index if exists {index_name}')


if __name__ == '__main__':
    from source.Tests import run_test_queries
    from source.RunIndexManagement import RunIndexManagement
    from source.StatementService import StatementService
    rim = RunIndexManagement()
    rim.db_conn.connect()
    rim.db_conn.set_stat_statements_extension()
    run_test_queries()
    records = rim.db_conn.get_stat_statements()
    ss = StatementService(records)
    ss.query_optimisation()
    ss.parse_query()
    query_list = ss.optimised_query_list
    for query_info in query_list:
        ic = IndexCreator(query_info.query, query_info.schema, query_info.tables_and_columns)
        ic.create_combination()
