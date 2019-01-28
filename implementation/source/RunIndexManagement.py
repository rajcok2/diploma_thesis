from source.DatabaseManager import Database
from source.Tests import run_test_queries
from source.StatementService import StatementService


class RunIndexManagement:
    def __init__(self):
        self.db_conn = Database()


if __name__ == '__main__':
    rim = RunIndexManagement()
    rim.db_conn.connect()
    rim.db_conn.set_stat_statements_extension()
    run_test_queries()
    records = rim.db_conn.get_stat_statements()
    ss = StatementService(records)
    ss.query_optimisation()
    ss.parse_query()
