from moz_sql_parser import parse
from source.DatabaseManager import Metadata


class StatementInfo:
    def __init__(self, record=None):
        self.query = record
        self.schema = None
        self.root = None
        self.main_query = None
        self.queries = dict()
        self.tables_and_columns = dict()
        self.table_aliases = dict()
        self.total_time = None
        self.metadata = None


class Node:
    def __init__(self):
        self.parent = None
        self.children = set()
        self.higher_nodes = set()
        self.query = None
        self.level = None

    def setup(self, _parent):
        self.level = _parent.level + 1
        self.parent = _parent
        self.higher_nodes = self.higher_nodes.union(_parent.higher_nodes)
        self.higher_nodes.add(_parent)


class StatementService:

    def __init__(self, queries):
        self.query_list = queries
        self.optimised_query_list = list()
        self.metadata = Metadata()

    def ignore_multiline_comments(self, query):
        start_char = '/*'
        end_char = '*/'
        while start_char in query:
            start = query.find(start_char)
            end = query.find(end_char)
            if end != -1:
                end += len(end_char)
                query = query[:start] + query[end:]

        while '--' in query:
            start = query.find('--')
            end = query.find('\n') + 2
            if start != -1:
                if end != -1:
                    query = query[:start] + query[end:]
                else:
                    query = query[:start]

        return query

    def query_optimisation(self):
        for query in self.query_list:
            q = query[0] = self.ignore_multiline_comments(query[0]).lower().replace(';', '')
            if 'my_table' in q and ('where' in q or 'on' in q):
                query_info = StatementInfo(q)
                query_info.root = self.parse_statement(q)
                query_info.queries = self.breadth_first_search(query_info.root)
                self.optimised_query_list.append(query_info)

    def parse_query(self):
        for query_info in self.optimised_query_list:
            level = 0
            queries = query_info.queries
            while level in queries:
                queries_set = queries[level]
                # print(".....................level:", level)
                for q in queries_set:
                    result = parse(q.query)

                    from_clause = result['from']
                    self.set_tables(from_clause, query_info)

                    query_info.metadata = self.metadata.get_metadata(query_info.tables_and_columns)
                    if 'where' in result:
                        where_clause = result['where']
                        self.set_columns(where_clause, query_info)

                    # print('cela query: ', query_info.query)
                    # print('rozparsovana query na subqueries: ', q.query)
                    # print('ich tabulky a stlpce: ', query_info.tables_and_columns)

                level += 1

    def parse_statement(self, query):
        stack = list()
        q = query.strip()
        node = Node()
        node.level = 0
        root = node
        index = 0

        while index < len(q):
            if q[index] == '(':
                stack.append(index)
                child = Node()
                child.setup(node)
                node.children.add(child)
                node = child
            elif q[index] == ')':
                start = stack.pop()
                end = index + 1
                sub_select = q[start+1:end-1]
                if 'select' in sub_select:
                    index = start
                    q = q[:start] + "?" + q[end:]
                    node.query = sub_select.replace("?", "'?'")
                else:
                    node.parent.children.remove(node)
                node = node.parent
            index += 1

        node.query = q.replace("?", "'?'")
        return root

    def breadth_first_search(self, root):
        queries = {0: {root}}
        queue = [root]
        visited = set()
        while queue:
            node = queue.pop()
            if node not in visited:
                visited.add(node)
                for child in node.children:
                    if child not in visited:
                        queue.append(child)
                        if child.level not in queries:
                            queries[child.level] = set()
                        sub_queries = queries[child.level]
                        sub_queries.add(child)

        return queries

    def set_columns(self, where_clause, q):
        def insert_column(item):
            table_name = None
            if '.' in item:
                if item.count('.') == 2:  # kvoli ON
                    schema, table_name, column_name = item.split('.')
                else:
                    table_name, column_name = item.split('.')

                if table_name not in q.tables_and_columns:
                    table_name = q.table_aliases[table_name]
                    if table_name not in q.tables_and_columns:
                        q.tables_and_columns[table_name] = set()
            else:
                column_name = item
                for key, columns_set in q.metadata.items():
                    if column_name in columns_set:
                        table_name = key

            q.tables_and_columns[table_name].add(column_name)

        stack = list()
        stack.append(where_clause)
        while stack:
            parent = stack.pop()
            for child in parent.values():
                for _item in child:
                    if type(_item) == dict:
                        stack.append(_item)
                    elif type(_item) == str and _item != '?':
                        insert_column(_item)

    def set_tables(self, from_clause, q):
        def get_table_name(string):
            if '.' in string:
                schema_name, string = string.split('.')
                q.schema = schema_name
                return string
            return string

        if type(from_clause) == dict:
            table_name = get_table_name(from_clause['value'])
            if table_name not in q.tables_and_columns:
                q.tables_and_columns[table_name] = set()
            q.table_aliases[from_clause['name']] = table_name

        elif type(from_clause) == list:
            for table in from_clause:
                _table = table
                if 'join' in table:
                    table = table['join']

                if type(table) == str:
                    table_name = get_table_name(table)
                    if table_name not in q.tables_and_columns:
                        q.tables_and_columns[table_name] = set()

                else:
                    table_name = get_table_name(table['value'])
                    if table_name not in q.tables_and_columns:
                        q.tables_and_columns[table_name] = set()
                    q.table_aliases[table['name']] = table_name

                if 'on' in _table:
                    self.set_columns(_table['on'], q)
        else:
            table_name = get_table_name(from_clause)
            if table_name not in q.tables_and_columns:
                q.tables_and_columns[table_name] = set()


if __name__ == '__main__':
    query = ("select *, (select count(*) from my_schema.my_table_jobs j where j.min_salary < e.salary) as how_many "
             "from  my_schema.my_table_employees as e "
             "where e.salary > (select avg(salary) from my_schema.my_table_employees); ")
    ss = StatementService([])
    ss.parse_statement(query)
