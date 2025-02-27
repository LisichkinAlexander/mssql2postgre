"""
Copy database from MS SQL to PostgreSQL module
"""
import sys
import datetime
import pyodbc
import psycopg2
import psycopg2.extensions
import settings_utils

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

def execute_sql(sql: str, conn_mssql: pyodbc.Connection, conn_postgre: psycopg2.extensions.connection) -> None:
    """ Execute sql """
    cursor_mssql = conn_mssql.cursor()
    cursor_postgre = conn_postgre.cursor()
    cursor_mssql.execute(sql)
    rows = cursor_mssql.fetchall()
    for row in rows:
        sql_text = row.SQL.replace('\r', '\n')
        sql = sql_text.replace('\r', '').replace('\n', '')
        sql_list  = sql.split(";")
        sql_text_list  = sql_text.split(";")
        for idx, sql in enumerate(sql_list):
            if sql:
                print(sql_text_list[idx])
                cursor_postgre.execute(sql)

def execute_script(json_data: dict, conn_mssql: pyodbc.Connection, conn_postgre: psycopg2.extensions.connection, script_name: str) -> None:
    """ Execute script_name """
    sql = json_data[script_name]
    execute_sql(sql, conn_mssql, conn_postgre)

def create_tables(json_data, conn_mssql: pyodbc.Connection, conn_postgre: psycopg2.extensions.connection) -> None:
    """ Create tables without indexes and constraint """
    cursor = conn_mssql.cursor()

    ms_objects = json_data['MsObjectsName_sql']
    cursor.execute(ms_objects)
    # Getting a list of objects (tables)
    rows = cursor.fetchall()

    sql_create = json_data['CreateTable_sql']

    # Create table
    for row in rows:
        if "'" not in row.name:
            sql = sql_create.replace("%s", row.name)
        else:
            sql = sql_create.replace("%s", row.name.replace("'", "''"))
        execute_sql(sql, conn_mssql, conn_postgre)

def get_pg_table_info(json_data, table_name: str, conn_postgre: psycopg2.extensions.connection):
    """ Getting information about a PostgreSQL table """
    pg_objects = json_data['PgObjectsName_sql']
    pg_objects = pg_objects.replace(':Table_Name', "'" + table_name+ "'")
    cursor = conn_postgre.cursor()
    cursor.execute(pg_objects)
    columns = [desc[0] for desc in cursor.description]
    row = cursor.fetchone()
    if row:
        return row[columns.index('row_count')], row[columns.index('column_count')]
    else:
        return None, None

def copy_data(json_data, conn_mssql: pyodbc.Connection, conn_postgre: psycopg2.extensions.connection) -> None:
    """ Copying data """

    def insert_to_db(cursor_postgre: pyodbc.Cursor, insert_sql: str, dict_params: dict, key_has_wrong_char: bool) -> None:
        """ Insert into db """
        if key_has_wrong_char:
            change_keys = []
            for key in dict_params.keys():
                if '(' in key or ')' in key:
                    new_key = key.replace('(', '_').replace(')', '_')
                    insert_sql = insert_sql.replace(f"%({key})", f"%({new_key})")
                    change_keys.append([new_key, key])
            for key in change_keys:
                dict_params[key[0]] = dict_params.pop(key[1])
        cursor_postgre.execute(insert_sql, dict_params)

    cursor_sql = conn_mssql.cursor()
    cursor_postgre = conn_postgre.cursor()

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
        row_count, column_count = get_pg_table_info(json_data, table_name, conn_postgre)
        pg_table_name = f"\"{table_name}\""
        if row_count and column_count and row_count == 0 and column_count == 0:
            continue
        if row_count is None and column_count is None:
            # Table not exist
            sql_create = json_data['CreateTable_sql']
            sql = sql_create.replace("%s", table_name)
            execute_sql(sql, conn_mssql, conn_postgre)
        elif column_count != row.column_count:
            # Table column changed - recreate table
            cursor_postgre.execute(f"drop table {pg_table_name}")
            sql_create = json_data['CreateTable_sql']
            sql = sql_create.replace("%s", table_name)
            execute_sql(sql, conn_mssql, conn_postgre)
        else:
            print(f"was row_count: {row_count} new row_count: {row.row_count}")
            cursor_postgre.execute(f"truncate table {pg_table_name}")

        fields_sql = json_data['FieldsMapping_sql']
        fields_sql = fields_sql.replace("%s", table_name)
        fields_data = cursor_sql.execute(fields_sql)
        fields_dict = {}
        for field in fields_data.fetchall():
            fields_dict[field.FIELD_NAME] = [field.SOURCE_TYPE, field.DESTINATION_TYPE]

        insert_sql = sql_insert.replace("%s", table_name)
        SQL = f"select * from [{table_name}] WITH(NOLOCK)"

        rows_data = cursor_sql.execute(SQL)
        columns = [column[0].replace("\r", '').replace("\n", '') for column in rows_data.description]
        sql_row = conn_mssql.execute(insert_sql).fetchone()
        insert_sql = sql_row.SQL.replace('\r', '').replace('\n', '')
        count = 0
        batch_count = 0
        batch_size = 50000 if 'BatchSize' not in json_data else int(json_data['BatchSize'])
        cursor_postgre.execute(f"ALTER TABLE {pg_table_name} SET UNLOGGED;")
        is_error = False
        try:
            cursor_postgre.execute("begin")
            try:
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
                            insert_to_db(cursor_postgre, insert_sql, dict_params, key_has_wrong_char)
                            print(f"inserted in batch {count} records: {int(count/row.row_count*100)}%")
                            dict_params = {}
                            batch_count = 0
                    # end for
                    if dict_params:
                        print(f"inserted in batch more {batch_count} records")
                        insert_to_db(cursor_postgre, insert_sql, dict_params, key_has_wrong_char)
                # end while
            except:
                is_error = True
                raise
            print("commit")
            cursor_postgre.execute("commit")
        finally:
            if not is_error:
                cursor_postgre.execute(f"ALTER TABLE {pg_table_name} SET LOGGED;")
        print(f"{datetime.datetime.now()}: inserted {count} data into {table_name}")
    print("All data copied")

def create_unique_constraint(json_data, conn_mssql: pyodbc.Connection, conn_postgre: psycopg2.extensions.connection) -> None:
    """ Create unique constraint """
    execute_script(json_data, 'CreateUniqueConstraint_sql', conn_mssql, conn_postgre)

def create_foreign_key(json_data, conn_mssql: pyodbc.Connection, conn_postgre: psycopg2.extensions.connection) -> None:
    """ Create foreign key constraint """
    execute_script(json_data, 'CreateForeignKey_sql', conn_mssql, conn_postgre)

def create_index(json_data, conn_mssql: pyodbc.Connection, conn_postgre: psycopg2.extensions.connection) -> None:
    """ Create indexes """
    execute_script(json_data, 'CreateIndex_sql', conn_mssql, conn_postgre)

def main() -> None:
    """
    Run coping
    """
    json_data = settings_utils.read_json_settings("mssql2postgre.json")
    conn_mssql, conn_postgre = connect(json_data)

    copy_functions = json_data["CopyFunctions"]
    this_module = sys.modules[__name__]
    for step in copy_functions:
        if settings_utils.strtobool(step["Execute"]):
            if "ScriptName" in step:
                params = (json_data, conn_mssql, conn_postgre, step["ScriptName"])
            else:
                params = (json_data, conn_mssql, conn_postgre)
            getattr(this_module, step["Function"])(*params)

if __name__ == "__main__":
    main()
