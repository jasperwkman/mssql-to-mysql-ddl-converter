import configparser
from db.sqlserver_db import SQLServerDB
from utils import ddl_convert
import traceback

CONFIG_FILE = 'config/db_config.ini'
TABLE_LIST_FILE = 'data/table_list.txt'
OUTPUT_FILE = 'output/output_ddl.sql' 

def main():
    # Read SQL Server config
    sqlserver_config = configparser.ConfigParser()
    sqlserver_config.read(CONFIG_FILE)

    sqlserver_params = dict(sqlserver_config['sqlserver'])

    # Open the file for reading
    with open(TABLE_LIST_FILE, "r") as file:
        # Read lines from the file and create tuples
        table_list = [line.strip() for line in file]

    # Create DB connections  
    sqlserver_db = SQLServerDB(sqlserver_params)
    
    try:
        table_meta = sqlserver_db.get_table_meta()
        table_pks = sqlserver_db.get_table_pk_cols()
        table_ddl = ddl_convert.generate_mysql_create_table_ddl(table_meta,table_pks)
        index_meta = sqlserver_db.get_index_meta()
        index_ddl = ddl_convert.generate_mysql_create_index_ddl(index_meta)
        with open(OUTPUT_FILE,'w') as sql_file:
            for table in table_ddl:
                if table in table_list:
                    print(f"Generating table DDL for table {table}")
                    #print(table_ddl[table])
                    sql_file.write(f"{table_ddl[table]}\n")
            for table_index in index_ddl:
                tb_name = table_index.split(".")[0]
                if tb_name in table_list:
                    print(f"Generating index DDL for {tb_name}.{table_index}")
                    #print(index_ddl[table_index])
                    sql_file.write(f"{index_ddl[table_index]}\n")
    except Exception as e:
        print(e)
        print(traceback.format_exc())


if __name__ == '__main__':
    main()