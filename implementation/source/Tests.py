from source.DatabaseManager import Database
import random


db = Database()

query_1 = "select first_name from my_schema.my_table_employees where first_name = 'John';"
query_2 = "select * from my_schema.my_table_countries c where region_id = 1;"
query_3 = "select * from my_schema.my_table_countries where country_name = 'Argentina';"
query_4 = "select * from my_schema.my_table_countries where country_name = 'Argentina' and region_id = 2;"
query_5 = "select * from my_schema.my_table_countries where region_id = 1 and country_name = 'Argentina';"
query_6 = "select * from my_schema.my_table_countries where region_id = 2 and country_id = 'AR' or  country_name = 'Argentina';"
query_7 = "select * from employees where employee_id = 101 --country_name='Argentina'\n;"
query_8 = "select * from employees where employee_id = 101;"
# query_9 = "select * \n" \
#           "from countries \n" \
#           "--join employees \n" \
#           "--on counties.id = employees.country_id \n" \
#           "where region_id = 1 --and country_id = 'eee' \n" \
#           ";"
# query_10 =  "select *\n" \
#             "from countries\n" \
#             "join locations\n" \
#             "on countries.country_id = locations.country_id\n" \
#             "where country_id = 'IT' and country_name = 'Argentina'\n" \
#             ";"
# query_11 = "select e.first_name from jobs join employees e on e.job_id = jobs.job_id where e.salary > jobs.min_salary;"
#
query_12 = "select e.first_name from my_schema.my_table_jobs j, my_schema.my_table_employees e " \
           "where e.job_id = j.job_id and e.salary > j.min_salary;"
#
# query_13 = "select first_name from my_schema.my_table_jobs j join my_schema.my_table_employees " \
#            "on my_schema.my_table_employees.job_id = j.job_id where my_schema.my_table_employees.salary > j.min_salary" \
#            " and j.job_id = 'FI_ACCOUNT';"
#
# query_14 = "select *, (select count(*) from my_schema.my_table_job_history), " \
#            "(select count(*) from my_schema.my_table_jobs j where j.min_salary < e.salary) as how_many " \
#            " from  my_schema.my_table_employees as e" \
#            " where e.salary > (select avg(salary) from my_schema.my_table_employees); " \
#            " select * from table where column = (select columns from tab limit 1);"

queries = [query_12]  # , query_2, query_3, query_4, query_5, query_6, query_12, query_13]


def run_test_queries():
    for i in range(100):
        q = random.choice(queries)
        db.execute_command(q)
