"""
Copy database from MS SQL to PostgreSQL module
"""
import os
import json
import datetime
import re
import pyodbc
import psycopg2
import psycopg2.extensions

def read_json() -> dict:
    """ Read setting json file """
    json_file = os.path.dirname(__file__) + "/mssql2postgre.json"
    if not os.path.exists(json_file):
        raise Exception("No setting json file mssql2postgre.json")
    with open(json_file, 'r', encoding="utf-8") as file:
        json_data = json.load(file)
    dict_sql = {}
    for key, value in json_data.items():
        if isinstance(value, str) and value.endswith(".sql"):
            sql_file = os.path.dirname(__file__) + "/SQL/" + value
            with open(sql_file, 'r', encoding="utf-8") as file:
                sql = file.read()
            dict_sql[key + "_sql"] = sql
        elif isinstance(value, str) and "@OS." in value:
            matches = re.findall(r"(?P<os_variable>\@OS\.(\w|\_)+)", value)
            if matches:
                for match in matches:
                    variable = match[0].replace('@OS.', '')
                    os_variable = os.environ.get(variable)
                    if os_variable:
                        value = value.replace(match[0], os_variable)
                        dict_sql[key] = value

    json_data.update(dict_sql)
    return json_data

def connect(json_data: dict):
    """ Connect to Databases """
    connect_string = json_data['MsSQLConnection']
    conn_mssql = pyodbc.connect(connect_string)
    print("Connect to MS SQL")
    connect_string = json_data['PostgresqlConnection']
    conn_postgre = psycopg2.connect(connect_string)
    print("Connect to Posgresql")
    conn_postgre.autocommit = True
    return conn_mssql, conn_postgre

def create_table(sql_create: str, table_name: str, conn_sql: pyodbc.Connection, conn_pstg: psycopg2.extensions.connection) -> None:
    """ Create table """
    cursor_sql = conn_sql.cursor()
    cursor_pstg = conn_pstg.cursor()
    print(table_name)
    if "'" not in table_name:
        SQL = sql_create.replace("%s", table_name)
    else:
        SQL = sql_create.replace("%s", table_name.replace("'", "''"))
    cursor_sql.execute(SQL)
    sql_row = cursor_sql.fetchone()
    SQL = sql_row.SQL.replace('\r', '\n')
    print(SQL)
    cursor_pstg.execute(SQL)

def create_tables(json_data, conn_sql: pyodbc.Connection, conn_pstg: psycopg2.extensions.connection) -> None:
    """ Create tables without indexes and constraint """
    cursor_sql = conn_sql.cursor()

    ms_objects = json_data['MsObjectsName_sql']
    cursor_sql.execute(ms_objects)
    # Getting a list of objects (tables)
    rows = cursor_sql.fetchall()

    sql_create = json_data['CreateTable_sql']

    # Create table
    for row in rows:
        create_table(sql_create, row.name, conn_sql, conn_pstg)

def get_pg_table_info(json_data, table_name: str, conn_pstg: psycopg2.extensions.connection):
    """ Getting information about a PostgreSQL table """
    pg_objects = json_data['PgObjectsName_sql']
    pg_objects = pg_objects.replace(':Table_Name', "'" + table_name+ "'")
    cursor_pstg = conn_pstg.cursor()
    cursor_pstg.execute(pg_objects)
    columns = [desc[0] for desc in cursor_pstg.description]
    row = cursor_pstg.fetchone()
    if row:
        return row[columns.index('row_count')], row[columns.index('column_count')]
    else:
        return None, None

def copy_data(json_data, conn_sql: pyodbc.Connection, conn_pstg: psycopg2.extensions.connection) -> None:
    """ Copying data """

    def insert_to_db(cursor_pstg: pyodbc.Cursor, ISQL: str, dict_params: dict, key_has_wrong_char: bool) -> None:
        """ Insert into db """
        if key_has_wrong_char:
            change_keys = []
            for key in dict_params.keys():
                if '(' in key or ')' in key:
                    new_key = key.replace('(', '_').replace(')', '_')
                    ISQL = ISQL.replace(f"%({key})", f"%({new_key})")
                    change_keys.append([new_key, key])
            for key in change_keys:
                dict_params[key[0]] = dict_params.pop(key[1])
        cursor_pstg.execute(ISQL, dict_params)

    cursor_sql = conn_sql.cursor()
    cursor_pstg = conn_pstg.cursor()

    ms_objects = json_data['MsObjectsName_sql']
    print("Get objects list")
    cursor_sql.execute(ms_objects)
    # Getting a list of objects (table) MS SQL
    rows = cursor_sql.fetchall()

    sql_insert = json_data['InsertTable_sql']

    # Create SQL Insert
    for row in rows:
        table_name = row.name
        if "'" in table_name:
            table_name = table_name.replace("'", "''")
        print(f"{datetime.datetime.now()}: {table_name}")
        row_count, column_count = get_pg_table_info(json_data, table_name, conn_pstg)
        pg_table_name = f"\"{table_name}\""
        if row_count and column_count and row_count == 0 and column_count == 0:
            continue
        if row_count is None and column_count is None:
            # Table not exist
            sql_create = json_data['CreateTable_sql']
            create_table(sql_create, table_name, conn_sql, conn_pstg)
        elif column_count != row.column_count:
            # Table column changed - recreate table
            cursor_pstg.execute(f"drop table {pg_table_name}")
            sql_create = json_data['CreateTable_sql']
            create_table(sql_create, table_name, conn_sql, conn_pstg)
        else:
            print(f"was row_count: {row_count} new row_count: {row.row_count}")
            cursor_pstg.execute(f"truncate table {pg_table_name}")

        fields_sql = json_data['FieldsMapping_sql']
        fields_sql = fields_sql.replace("%s", table_name)
        fields_data = cursor_sql.execute(fields_sql)
        fields_dict = {}
        for field in fields_data.fetchall():
            fields_dict[field.FIELD_NAME] = [field.SOURCE_TYPE, field.DESTINATION_TYPE]

        ISQL = sql_insert.replace("%s", table_name)
        SQL = f"select * from [{table_name}] WITH(NOLOCK)"

        rows_data = cursor_sql.execute(SQL)
        columns = [column[0].replace("\r", '').replace("\n", '') for column in rows_data.description]
        sql_row = conn_sql.execute(ISQL).fetchone()
        ISQL = sql_row.SQL.replace('\r', '').replace('\n', '')
        count = 0
        batch_count = 0
        batch_size = 50000 if 'BatchSize' not in json_data else int(json_data['BatchSize'])
        cursor_pstg.execute(f"ALTER TABLE {pg_table_name} SET UNLOGGED;")
        try:
            cursor_pstg.execute("begin")
            dict_params = {}
            while True:
                cursor = rows_data.fetchmany(batch_size)
                if not cursor:
                    break
                key_has_wrong_char = False
                for row_data in cursor:
                    dict_row = dict(zip(columns, row_data))
                    for key, value in dict_row.items():
                        if '(' in key or ')' in key:
                            key_has_wrong_char = True
                        if key in fields_dict and value and \
                            (fields_dict[key][0] in ["TEXT", "VARCHAR", "NTEXT", "NVARCHAR"] and fields_dict[key][1] == "BYTEA"):
                            value = bytearray(value, "utf8")
                        if key in dict_params:
                            dict_params[key].append(value)
                        else:
                            dict_params[key] = [value]
                    count += 1
                    batch_count += 1
                    if count > 0 and count % batch_size == 0:
                        insert_to_db(cursor_pstg, ISQL, dict_params, key_has_wrong_char)
                        print(f"inserted in batch {count} records: {int(count/row.row_count*100)}%")
                        dict_params = {}
                        batch_count = 0
                # end for
                if dict_params:
                    print(f"inserted in batch more {batch_count} records")
                    insert_to_db(cursor_pstg, ISQL, dict_params, key_has_wrong_char)
            # end while
            print("commit")
            cursor_pstg.execute("commit")
        finally:
            cursor_pstg.execute(f"ALTER TABLE {pg_table_name} SET LOGGED;")
        print(f"{datetime.datetime.now()}: inserted {count} data into {table_name}")
    print("All data copied")

def main() -> None:
    """
    Run coping
    """
    json_data = read_json()
    conn_sql, conn_pstg = connect(json_data) 

    #create_tables(json_data, conn_sql, conn_pstg)
    copy_data(json_data, conn_sql, conn_pstg)

if __name__ == "__main__":
    main()
