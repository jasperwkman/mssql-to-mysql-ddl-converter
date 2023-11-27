# utils.py
from .mssql_to_mysql_map import mssql_to_mysql_map

def convert_sql_type(mssql_type: str, length: int = None, precision: int = 0, data_max_length: int = None) -> str:
    mysql_type = mssql_to_mysql_map[mssql_type]
    return_type = mysql_type

    if mysql_type.find(',') > 0:
        return_type = f'{mysql_type.split(",")[0]}({length})'
        type_config = mysql_type.split(',')
        if length == -1: #if varchar/nvarchar[max]
            return_type = type_config[-1].split('|')[1]
        else:
            for i in range(1,len(type_config)):
                max_len,max_type = mysql_type.split(',')[i].split("|")
                if length > int(max_len):
                    return_type = max_type

    elif mysql_type.find(":") > 0:
        max_len,max_type = mysql_type.split(":")[1].split("|")
        if data_max_length > int(max_len):
            return_type = max_type
        else:
            return_type = mysql_type.split(":")[0]

    if mysql_type == 'decimal':
        return_type = f'decimal({length},{precision})'

    return return_type

def remove_parentheses(s):
    if s.startswith('('):
            s = s[1:]
    if s.endswith(')'):
            s = s[:-1]
    return s
def generate_mysql_create_index_ddl(metadata):
    ddl_statements = {}
    for table_index_name, columns in metadata.items():
        table, index = table_index_name.split(".")
        if columns['PrimaryKey'] == 1:
            ddl_statements[table_index_name] = f"ALTER TABLE {table} ADD CONSTRAINT {index} PRIMARY KEY ({','.join(columns['Columns'])});"
        elif columns['Fulltext'] == 'Yes':
            #CREATE FULLTEXT INDEX ceo_log_FTIDX on ceo_log(log_subject,log_detail,log_remark);
            ddl_statements[table_index_name] = f"CREATE FULLTEXT INDEX {index} ON {table}({columns['FulltextColumns']});"
        else:
            ddl_statements[table_index_name] = f"CREATE {'UNIQUE' if columns['Unique'] == 1 or columns['Clustered'] == 'CLUSTERED' else ''} INDEX {index} ON {table}({','.join(columns['Columns'])});"
    return ddl_statements

def generate_mysql_create_table_ddl(metadata,table_pks):
    ddl_statements = {}

    for table_name, columns in metadata.items():
        create_table_statement = f"CREATE TABLE {table_name} (\n"
        column_statements = []
        auto_increment = ""
        for column in columns:
            column_name = column['Column']
            data_type = column['DataType']
            length = column['Length']
            precision = column['PrecisionScale']
            is_nullable = column['IsNnullable']
            column_default = column['ColumnDefault']
            data_max_length = column['DataMaxLength']
            is_identity = column['IsIdentity']
            identity_value = column['IdentityValue']
            # Convert SQL Server data type to MySQL data type
            mysql_data_type = convert_sql_type(data_type, length, precision,data_max_length)

            # Create column statement
            column_statement = f"{column_name} {mysql_data_type}"
            #print(data_type,length,mysql_data_type)
            # Add NOT NULL constraint if the column is not nullable
            if not is_nullable:
                    column_statement += " NOT NULL"

            if is_identity == 1:
                column_statement += " AUTO_INCREMENT"
                auto_increment = f"AUTO_INCREMENT={identity_value}"
            # Add DEFAULT constraint if a default value is provided
            # BLOG,TEXT,GEOMETRY or JSON column cannot have a default value in mysql
            elif column_default and mysql_data_type not in ['blob','text','longtext','geometry','json']:
                if column_default == '(getdate())':
                    if mysql_data_type == 'timestamp':
                        column_default = 'CURRENT_TIMESTAMP'
                    elif mysql_data_type == 'datetime':
                        column_default = 'NOW()'
                    elif mysql_data_type == 'datetime(3)':
                        column_default = 'CURRENT_TIMESTAMP(3)'
                else:
                    column_default = remove_parentheses(column_default)
                column_statement += f" DEFAULT {column_default}"

            column_statements.append(column_statement)

        # Join column statements
        create_table_statement += ", \n".join(column_statements)
        if table_pks[table_name]:
            create_table_statement += f",\nPRIMARY KEY ({table_pks[table_name]})"
        create_table_statement += f"\n) {auto_increment};"

        ddl_statements[table_name] = create_table_statement

    return ddl_statements

if __name__ == "__main__":
    print(convert_sql_type('varchar', 300))