import pyodbc
import traceback
import getpass

class SQLServerDB:

    def __init__(self, params):

        self.host = params['hostname']
        self.port = params['port']
        try:
            self.username = params['username']
        except:
            self.username = ""
        try:
            self.password = params['password']
        except:
            self.password = ""
        self.db = params['db_name']
        if self.username == "":
            input("Enter database username: ")
        if self.password == "":
            getpass.getpass("Enter database password: ")

        conn_str = f'DRIVER={{SQL Server}};SERVER={self.host},{self.port};DATABASE={self.db};UID={self.username};PWD={self.password}'

        self.conn = pyodbc.connect(conn_str)

    def query(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)

        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

        return [dict(zip(columns, row)) for row in data]

    def get_index_meta(self):
        cursor = self.conn.cursor()

        query = """
select g1.TableName as 'Table', g1.IndexName as 'Index',
g1.ColumnName as 'Column', g1.Sequence as 'Sequence', g1.IsUnique as 'Unique', 
g1.PrimaryKey, g1.IsClustered as 'Clustered', g1.Fulltext, g2.FTColumns from 
(
SELECT
	t.name AS 'TableName',
	i.name AS 'IndexName',
	c.name AS 'ColumnName',
	ic.key_ordinal AS 'Sequence',
	i.is_unique AS 'IsUnique',
	i.is_primary_key AS 'PrimaryKey',
	i.type_desc AS 'IsClustered',
	CASE
    	WHEN ft.unique_index_id IS NULL THEN 'No'
    	ELSE 'Yes'
	END AS 'Fulltext'
FROM
	sys.indexes i
JOIN
	sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN
	sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
JOIN
	sys.tables t ON i.object_id = t.object_id
LEFT JOIN
	sys.fulltext_indexes ft ON i.object_id = ft.object_id AND i.index_id = ft.unique_index_id
WHERE
	t.is_ms_shipped = 0 and i.is_primary_key = 0

) as g1
LEFT OUTER JOIN 
(
SELECT
    t.name AS 'TableName',
    i.name AS 'IndexName',
    STUFF(
   	 (
   		 SELECT ', ' + cl.name
   		 FROM sys.fulltext_index_columns ic
   		 INNER JOIN sys.columns cl ON ic.column_id = cl.column_id AND ic.[object_id] = cl.[object_id]
   		 WHERE ic.[object_id] = t.[object_id]
   		 FOR XML PATH('')
   	 ), 1, 2, ''
    ) AS 'FTColumns'
FROM
    sys.objects t
INNER JOIN
    sys.fulltext_indexes fi ON t.[object_id] = fi.[object_id]
INNER JOIN
    sys.fulltext_catalogs c ON fi.fulltext_catalog_id = c.fulltext_catalog_id
INNER JOIN
    sys.indexes i ON fi.unique_index_id = i.index_id AND fi.[object_id] = i.[object_id]
GROUP BY
    t.name,
    i.name,
    t.[object_id]
)
AS g2 on g1.TableName = g2.TableName and g1.IndexName = g2.IndexName
ORDER BY
	g1.TableName, g1.IndexName, g1.Sequence;
    """
        result = {}
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                table_index = f"{row.Table}.{row.Index}"
                #print(row)
                if table_index not in result:
                    result[table_index] = {}
                    result[table_index]['Unique'] = row.Unique
                    result[table_index]['Clustered'] = row.Clustered
                    result[table_index]['PrimaryKey'] = row.PrimaryKey
                    result[table_index]['Fulltext'] = row.Fulltext
                    result[table_index]['FulltextColumns'] = row.FTColumns
                    result[table_index]['Columns'] = []
                result[table_index]['Columns'].append(row.Column)

        except Exception as e:
            print(e)
            print(traceback.format_exc())

        return result

    def get_table_meta(self):

        cursor = self.conn.cursor()

        query = """
        SELECT  
            t.name AS 'Table',
            c.name AS 'Column',  
            c.column_id AS 'Sequence',
            ty.name AS 'DataType',
			CASE 
				WHEN ty.name = 'nvarchar' THEN c.max_length/2
				WHEN ty.name = 'nvhar' THEN c.max_length/2
				ELSE c.max_length
			END as 'Length',
			c.scale AS 'PrecisionScale',
            c.is_nullable AS 'IsNnullable',
			c.is_identity AS 'IsIdentity',
			CASE
				WHEN c.is_identity = 1 THEN IDENT_CURRENT(OBJECT_SCHEMA_NAME(t.object_id, db_id()) + '.' + t.name) + IDENT_INCR(OBJECT_SCHEMA_NAME(t.object_id, db_id()) + '.' + t.name)
				ELSE 0
			END AS 'IdentityValue',
            CAST(ISNULL(OBJECT_DEFINITION(dc.object_id), '') AS varchar(100))  AS 'ColumnDefault'
        FROM   
            sys.tables t
        JOIN   
            sys.columns c ON t.object_id = c.object_id
        JOIN
            sys.types ty ON c.system_type_id = ty.system_type_id
        LEFT JOIN
            sys.default_constraints dc ON c.default_object_id = dc.object_id
        WHERE
            t.is_ms_shipped = 0 AND ty.name != 'sysname'
        ORDER BY  
            t.name, c.column_id;
    """

        cursor.execute(query)
        rows = cursor.fetchall()

        result = {}
        for row in rows:
            table = row.Table
            #print(row)
            if table not in result:
                result[table] = []
            r = {}
            r['Column'] = row.Column
            r['DataType'] = row.DataType
            r['Length'] = row.Length
            r['PrecisionScale'] = row.PrecisionScale
            r['IsNnullable'] = row.IsNnullable
            r['ColumnDefault'] = row.ColumnDefault
            r['IsIdentity'] = row.IsIdentity
            r['IdentityValue'] = row.IdentityValue
            r['DataMaxLength'] = None
            if row.DataType in ['text','ntext','binary','varbinary']:
                r['DataMaxLength'] = self.get_column_max_length(table,row.Column)
            #print(row.ColumnDefault)
            result[table].append(r)

            
        return result

    def get_table_pk_cols(self):

        cursor = self.conn.cursor()
        query = """
        SELECT
            t.name AS 'TableName',
            STUFF((SELECT ',' + c.name
                FROM sys.indexes i
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE i.is_primary_key = 1 AND t.object_id = i.object_id
                ORDER BY ic.key_ordinal
                FOR XML PATH('')), 1, 1, '') AS 'PKColumns'
        FROM
            sys.tables t
        ORDER BY
            t.name;
    """
        cursor.execute(query)
        rows = cursor.fetchall()
        result = {}
        for row in rows:
            result[row.TableName] = row.PKColumns
        return result

    def get_column_max_length(self,table_name,column_name):
        cursor = self.conn.cursor()

        query = f"SELECT top 1 DATALENGTH({column_name}) as max_len from {table_name} order by DATALENGTH({column_name}) DESC;"

        cursor.execute(query)
        row = cursor.fetchone()
        if row != None and row.max_len != None:
            return row.max_len
        else:
            return 0