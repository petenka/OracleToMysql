# -*- coding: utf-8 -*-
import mysql.connector
import cx_Oracle
import os
import codecs

#os.environ["IDE_PROJECT_ROOTS"] = "c:\Python27_11"
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"

conf = open('config.txt', 'r')

'ORACLE'
conf.readline()
oracle_user = conf.readline()
oracle_password = conf.readline()
oracle_host = conf.readline()
oracle_port = conf.readline()
oracle_db = conf.readline()
config_oracle = oracle_user[:-1]+'/'+oracle_password[:-1]+'@'+oracle_host[:-1]+':'+oracle_port[:-1]+'/'+oracle_db[:-1]

'SQL'
conf.readline()
sql_user = conf.readline()
sql_password = conf.readline()
sql_host = conf.readline()
sql_db = conf.readline()

conf.close()

conn_oracle = cx_Oracle.connect(config_oracle)
cursor_oracle = conn_oracle.cursor()

conn_sql = mysql.connector.connect(user=sql_user[:-1],password=sql_password[:-1],host=sql_host[:-1],database=sql_db[:-1],)
cursor_sql = conn_sql.cursor()

#f = open('log.txt', 'w')
f = codecs.open('log.txt','w',encoding='utf8')

TABLE_NAME = 'MY_TABLE'

TABLE_INFO_SQL = '''SELECT table_name, lower(column_name), data_type, nullable,
    decode(default_length, NULL, 0, 1) hasdef,
    decode(
        data_type,
        'DATE', '11',
        'NUMBER', nvl(data_precision,data_length) || ',' || nvl(data_scale,0),
        data_length
    ) data_length,
    data_default
    FROM user_tab_columns
    WHERE INSTR(table_name, \'X_\') <> 1 AND INSTR(table_name, \'$\') = 0
    AND table_name = \''''+TABLE_NAME+ '''\'
    ORDER BY table_name, column_id
    '''

PRIMARY_KEYS_INFO_SQL = '''SELECT uc.table_name, lower(ucc.column_name)
FROM user_constraints uc, user_cons_columns ucc
WHERE uc.constraint_name = ucc.constraint_name
AND uc.constraint_type = 'P'
AND uc.table_name = \''''+TABLE_NAME+ '''\'
'''

AURO_INCREMENT_INFO_SQL = '''
SELECT trigger_name, trigger_type, triggering_event, table_name, trigger_body
FROM user_triggers
WHERE INSTR(table_name, 'X_') <> 1
AND INSTR(table_name, '$') = 0
AND table_name = \''''+TABLE_NAME+ '''\'
'''

INDEXES_INFO_SQL = '''SELECT ui.index_name, ui.uniqueness, ic.COLUMN_NAME, tc.data_type, tc.data_length
FROM user_indexes ui, user_ind_columns ic
Left join user_tab_columns tc on tc.COLUMN_NAME=ic.COLUMN_NAME  and tc.TABLE_NAME = ic.TABLE_NAME
WHERE INSTR(ui.table_name, 'X_') <> 1
AND INSTR(ui.table_name, '$') = 0
and ui.INDEX_NAME = ic.INDEX_NAME
AND ui.table_name = \''''+TABLE_NAME+ '''\'
'''

column_oracle_ai = 'null'
column_dataTypes = {}
cursor_oracle.execute(AURO_INCREMENT_INFO_SQL)
for row in cursor_oracle:
    if(row[2]=='INSERT'):
        body = row[4]
        position = body.find(':NEW.')
        position2 = body.find(' ',position)
        column_oracle_ai = body[position+5:position2]

cursor_oracle.execute(TABLE_INFO_SQL)
query_sql = ''
column_count = 0
for row in cursor_oracle:
    '''print row[1] + " " + row[2] + " " + row[3]+ " ",
    print  row[4],
    print  row[5],
    print  row[6]'''
    if (query_sql==''):
        query_sql = "CREATE TABLE "+row[0] +" \n( "

    'COLUMN NAME'
    query_sql +=  row[1]  +" "

    'DATA TYPE'
    if (row[2]== 'NUMBER'):
        column_dataTypes[column_count] = 'n'
        query_sql +=  ' FLOAT ('+ row[5]+ ')'
    elif (row[2]== 'DATE') or ('TIMESTAMP' in row[2]):
        column_dataTypes[column_count] = 'd'
        query_sql +=  ' DATETIME '
    elif (row[2]== 'CLOB'):
        column_dataTypes[column_count] = 's'
        query_sql +=  ' TEXT '
    elif (row[2]== 'CHAR'):
        column_dataTypes[column_count] = 's'
        query_sql +=  ' CHAR('+ str(row[5])+') '
    elif (row[2]== 'VARCHAR2') or (row[2]== 'NVARCHAR2'):
        column_dataTypes[column_count] = 's'
        query_sql +=  ' VARCHAR('+ str(row[5])+') '

    'UNSIGNED'

    'NULL/NOT NULL'
    if (row[3] == 'Y'):
        query_sql += ' '
    else:
        query_sql += ' NOT NULL '

    'DEFAULT VALUE'
    if (row[6] is None):
        mydefault = ''
    else:
        mydefault = str(row[6])

        if ("sysdate" in mydefault):
            query_sql += ' DEFAULT CURRENT_TIMESTAMP '
        else:
            query_sql += ' DEFAULT '+ mydefault

    'AUTO INCREMENT'
    if(column_oracle_ai != 'null'):
        if(row[1]==column_oracle_ai):
            query_sql += ' AUTO_INCREMENT '


    query_sql += ',\n'
    column_count = column_count+1


cursor_oracle.execute(PRIMARY_KEYS_INFO_SQL)
for row in cursor_oracle:
    query_sql += ' PRIMARY KEY ('+row[1]+') , '

cursor_oracle.execute(INDEXES_INFO_SQL)
for row in cursor_oracle:
    if(row[1]=='UNIQUE'):
        if ((row[3]=='VARCHAR2') and (row[4]>250)):
            query_sql += 'UNIQUE KEY '+row[2]+' ('+row[2]+'(250)), '
            print 'Warning: Unique key '+row[2]+' will be set to only 250 bytes.\n'
            f.write('Warning: Unique key '+row[2]+' will be set to only 250 bytes.\n')
        else:
            query_sql += 'UNIQUE KEY '+row[2]+' ('+row[2]+'), '

query_sql = query_sql[:-2]
query_sql += ');'

print query_sql
print ''
f.write(query_sql)
f.write('\n\n')
cursor_sql.execute(query_sql)

'-----------------------------------------------------------------------------'

'DATA'
#cursor_sql.execute("SET NAMES 'UTF8';")
#cursor_sql.execute("SET CHARACTER SET 'UTF8';")
#cursor_sql.execute("SET character_set_connection='UTF8';")

oracle_select = 'select * from ' + TABLE_NAME
cursor_oracle.execute(oracle_select)

for row in cursor_oracle:
    sql_insert = u' insert into '+ TABLE_NAME + ' values ('
    for i in range(0,column_count):
        if (row[i] is None):
            sql_insert += 'null, '
        else:
            if (column_dataTypes[i]=='s'):
                myValue = row[i].decode('utf8')
                myValue = myValue.replace('"', '\\"')
                sql_insert += '"'+myValue + '", '
            elif(column_dataTypes[i]=='d'):
                sql_insert += '"'+str(row[i]) + '", '
            else:
                sql_insert += str(row[i]) + ', '

    sql_insert = sql_insert[:-2]
    sql_insert +=' );'
    f.write(sql_insert)
    f.write('\n')
    cursor_sql.execute(sql_insert)
    
    print cursor_sql._executed


conn_sql.commit()

f.close()
cursor_oracle.close()
conn_oracle.close()
cursor_sql.close()
conn_sql.close()
