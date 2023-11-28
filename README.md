# MSSQL to MySQL DDL Converter

## Overview

This is a Python application designed to connect to a Microsoft SQL Server, retrieve the table and index Data Definition Language (DDL), and convert it to DDL compatible with MySQL.
I found there are no such free tools to do so, so this tool simplifies the migration process for users looking to transition their database schemas from SQL Server to MySQL.
The Dataype basicly referenced <a href="https://dev.mysql.com/doc/workbench/en/wb-migration-database-mssql-typemapping.html">MySQL Workbench Manual</a>, plus enhancements.

## Features

- Connects to a SQL Server database.
- Retrieves table and index DDL statements.
- Converts SQL Server DDL to MySQL-compatible DDL.
- Generates DDL scripts for specified tables.
- Support Table, Index, Primary key , Default column value

## Prerequisites

Before using the MSSQL to MySQL DDL Converter, ensure you have the following dependencies installed:

- Python 3.7
- Install dependencies using the following command:
  ```bash
  pip install -r requirements.txt

## Configuration

The application requires configuration files to specify the database connection details and the list of tables for which DDL scripts will be generated.

### Database Configuration (config/db_config.ini)
```ini
[sqlserver]
hostname = localhost
port = 1433
# username and password will be prompted at runtime if not provided here
username = db_user
password = db_pass
```

## Table List (data/table_list.txt)
Specify the tables for which DDL scripts should be generated. Add one table name per line:
```plaintext
table_1
table_2
```

## Usage

1. Configure the database connection details in `config/db_config.ini`.
2. Update the list of tables in `data/table_list.txt`.
3. Run the script using the following command:

    ```bash
    python your_script_name.py
    ```

   If you haven't provided a username or password in the configuration file, the script will prompt you for them during runtime.
4. DDL for MySQL will be result in output/output_ddl.sql
