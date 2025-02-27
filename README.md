mssql2postgre - the database migration tool from MS SQL Server to PostgreSQL.

mssql2postgre is a tool, intended to make a process of migration from MS SQL Server to PostgreSql as easy and smooth as possible.

KEY FEATURES

    Ease of use - the only thing needed to run this script is the Python interpreter.
    Accuracy of migration the database structure - mssql2postgre converts MS SQL data types to corresponding PostgreSql data types, creates constraints, indexes, primary and foreign keys exactly as they were before migration.
    Speed of data transfer - in order to migrate data fast
    Ease of monitoring - FromMySqlToPostgreSql will provide detailed output about every step, it takes during the execution.
    Ease of configuration - all the parameters required for migration should be put in one single json file.
    Ability to transfer only a data (in case of an existing database).

KEY FEATURES

	Ease of use - the only thing needed to run this script is the Python
	Script support MS SQL user-defined data types
	
SYSTEM REQUIREMENTS

    Python 3.10 or above
    psycopg2 should be installed
    pyodbc should be installed 
    jsonschema should be installed 
    all required library can be installed using requirements.txt
	pip install -r requirements.txt.

USAGE

1. Create a new database.
    Sample:CREATE DATABASE my_postgresql_database;

2. Download mssql2postgre.

3. Change configuration mssql2postgre.json file according to your requirements:
    MsSQLConnection - connection string to MS SQL Server
	PostgresqlConnection - connection string to PostgreSql
	You can store server address/user name/password in json file or on OS variable:
		MS_SQL_SERVER: MS SQL server address
		MS_SQL_DATABASE: MS SQL export database name
		MS_SQL_USER: MS SQL user
		MS_SQL_PASSWORD: MS SQL user password
		POSTGRESQL_SERVER: PostgreSql server address
		POSTGRESQL_DATABASE: PostgreSql database name
		POSTGRESQL_USER: PostgreSql user
		POSTGRESQL_PASSWORD: PostgreSql user password

4. Run the script from a terminal.
    Sample:
   python mssql2postgre.py

5. At the end of migration check log messages.

6. In case of any remarks, misunderstandings or errors during migration,
    please feel free to email me lisichkin.alexander@gmail.com

Tested using MS SQL Server 12 (2014) and PostgreSql (16.6).
