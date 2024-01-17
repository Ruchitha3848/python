import gzip
import json
import os
import re

from flask import Flask, Response, render_template, request, url_for,redirect
import numpy as np
import snowflake.connector
import pandas as pd
import pyodbc
import xml.etree.ElementTree as ET
import io
from azure.storage.blob import BlobServiceClient
import math
from Single_Table_Validation_without_primary import generate_report

app = Flask(__name__)
@app.route("/")
def input():
    return render_template('inputpage_try.html')

@app.route("/validate",methods=['POST','GET'])
def validate():
    validation_type = request.form.get('validation_type', default='abc')
    file_input = request.form.get('file_input', default='abc')
    source_password = request.form.get('source_password', default='abc')
    user_query_source = request.form.get('user_query_source', default='abc')
    file_name = request.form.get('file_name', default='abc')
    schema1 = request.form.get('schema1', default='abc')
    database = request.form.get('database', default='abc')
    table = request.form.get('table', default='abc')
    schema2 = request.form.get('schema2', default='abc')
    boolean_input = request.form.get('boolean_input', default='abc')
    user_query_target = request.form.get('user_query_target', default='abc')
    password = request.form.get('password', default='abc')

    
    response=generate_report(file_input,file_name,database,table,password,source_password)
    print("----------------------------------------------------")
   
        
            
        
    if isinstance(response, Response) and response.status_code == 200:
                # Extract the context from the JSON response
            
        context = response.get_json()
            
        return render_template( "outputpage.html",context=context)
    else:
        return "Error: Unable to generate report", 500
   
        
    

@app.route('/generat_report',methods=['POST','GET'])
def generat_report():
    print("ppppppppppppppppppppppppppppppppppppppppppp")
    global html_reports,unmatched_reports
    html_reports = []  
    unmatched_reports = []
    validation_type = request.form.get('validation_type', default='abc')
    file_input = request.form.get('file_input', default='abc')
    source_password = request.form.get('source_password', default='abc')
    user_query_source = request.form.get('user_query_source', default='abc')
    file_name = request.form.get('file_name', default='abc')
    schema1 = request.form.get('schema1', default='abc')
    database = request.form.get('database', default='abc')
    table = request.form.get('table', default='abc')
    schema2 = request.form.get('schema2', default='abc')
    boolean_input_value = request.form.get('boolean_input', default='abc')
    user_query_target = request.form.get('user_query_target', default='abc')
    password = request.form.get('password', default='abc')
   
   

    # file_input= file_input
    # file_name =file_name
    # database = database
    # table = table
    # password = password
    # source_password = source_password
    # validation_type = validation_type
    # user_query_source = user_query_source
    # user_query_target = user_query_target
    # schema1=schema1
    # schema2=schema2
    # boolean_input_value=boolean_input
    print("nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn")
    if(validation_type =="schema"):
        print("mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm")
   
        if(database == "SQL_Server"):
            data_1 = pd.read_excel('C:\TNDV\Schema\SQL_Server_Connection.xlsx',sheet_name='SQL_Server_Connection-1')
    
            connection_dict = {}
            for index, row in data_1.iterrows():
                connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
            (connection_dict)
            for key in connection_dict:
                
                if key == "server":
                    server_sq = connection_dict[key]
                    
                elif key =="database":
                    database_sq = connection_dict[key]
                    
                elif key =="username":
                    username_sq = connection_dict[key]


            password_sq = password
            print("Database  is :   ",database_sq)
            print("Password is :    ",password_sq)
            print("Username  is :   ",username_sq)

            
            if(file_input =="SQL_Server"):

                conn1 = pyodbc.connect(
                        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server_sq + ';DATABASE=' + database_sq + ';UID=' + username_sq + ';PWD=' + password_sq
                    )

          

                data_2 = pd.read_excel('C:\TNDV\Schema\SQL_Server_Connection.xlsx',sheet_name='SQL_Server_Connection-2')
             
                connection_dict = {}
                for index, row in data_2.iterrows():
                    connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
               
                for key in connection_dict:
                   
                    if key == "server":
                        server_sq = connection_dict[key]
                        
                    elif key =="database":
                        database_sq = connection_dict[key]
                        
                    elif key =="username":
                        username_sq = connection_dict[key]


                password_sq_2 = source_password
                print("Database  is :   ",database_sq)
                print("Password is :    ",password_sq_2)
                print("Username  is :   ",username_sq)

                schema1_name = file_name
                schema2_name = table
                conn2 = pyodbc.connect(
                        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server_sq + ';DATABASE=' + database_sq + ';UID=' + username_sq + ';PWD=' + password_sq
                    )
                

                def get_primary_keys_sql_server(connection, schema_name, table_name):
                    query = """
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
                        AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    """
                 
                    primary_keys_df = pd.read_sql(query, connection, params=[schema_name, table_name])
               
                    return primary_keys_df['COLUMN_NAME'].tolist()



                tables_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?"
             
                tables_df1 = pd.read_sql(tables_query, conn1, params=[schema1_name])
                tables_df2 = pd.read_sql(tables_query, conn2, params=[schema2_name])

                sql_server_tables1 = tables_df1['TABLE_NAME'].tolist()
                sql_server_tables2 = tables_df2['TABLE_NAME'].tolist()


                count_tables_source = len(sql_server_tables1)
                count_tables_target = len(sql_server_tables2)


                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0

                for table_name1 in sql_server_tables1:
                    primary_keys_sql_1 = get_primary_keys_sql_server(conn1, schema1_name, table_name1)

                    found_match = False
                    for table_name2 in sql_server_tables2:
                        primary_keys_sql_2 = get_primary_keys_sql_server(conn2, schema2_name, table_name2)

                        if primary_keys_sql_1 == primary_keys_sql_2:
                            found_match = True
                  

                            query1 = f"SELECT * FROM [{schema1_name}].[{table_name1}]"
                            query2 = f"SELECT * FROM [{schema2_name}].[{table_name2}]"

                            df_sql_1 = pd.read_sql(query1, conn1)
                            df_sql_2 = pd.read_sql(query2, conn2)


                            df_sql_1 = df_sql_1.astype(str)
                            df_sql_2 = df_sql_2.astype(str)
                            

                            comparison = df_sql_1.equals(df_sql_2)
        
                        
                            unmatched_rows_sql_1 = df_sql_1[~df_sql_1.isin(df_sql_2)].dropna(how='all')
                            for col in primary_keys_sql_1:
                                unmatched_rows_sql_1[col] = df_sql_1[col]
                            unmatched_rows_sql_1.index = unmatched_rows_sql_1.index + 1  
                            unmatched_rows_sql_1 = unmatched_rows_sql_1[~unmatched_rows_sql_1.isin(primary_keys_sql_1)].dropna(how='all')

                          
                            unmatched_rows_sql_2 = df_sql_2[~df_sql_2.isin(df_sql_1)].dropna(how='all')
                            for col in primary_keys_sql_2:
                                unmatched_rows_sql_2[col] = df_sql_2[col]
                            unmatched_rows_sql_2.index = unmatched_rows_sql_2.index + 1  
                            unmatched_rows_sql_2 = unmatched_rows_sql_2[~unmatched_rows_sql_2.isin(primary_keys_sql_2)].dropna(how='all')
                           

                            total_rows_sql_1 = len(df_sql_1)
                            total_rows_sql_2 = len(df_sql_2)

                            count_unmatched_sql_1 = 0
                            count_unmatched_sql_2 = 0


                            if not comparison:
                                count_unmatched_tables_source += 1
                                count_unmatched_tables_target += 1

                                count_unmatched_sql_1 =len(unmatched_rows_sql_1.index)
                                count_unmatched_sql_2 =len(unmatched_rows_sql_2.index)


                            print(f"The data in the {table_name1} dosent matches the data in the   {table_name2} .")
                     
                            

                            unmatched_rows_sql_1_html = unmatched_rows_sql_1.to_html(na_rep='-', escape=False)
                            unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                            unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace('<tr>', '<tr style="text-align: center">')

                        

                            for index, row in unmatched_rows_sql_1.iterrows():
                                for col, value in row.items():
                                    
                                    if col in primary_keys_sql_1:
                                                unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                                    elif ( value != '-' and col not in primary_keys_sql_1):
                                                unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

                        


                            # Convert unmatched_rows_sql_2 to HTML with background colors
                            unmatched_rows_sql_2_html = unmatched_rows_sql_2.to_html(na_rep='-', escape=False)
                            unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                            unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace('<tr>', '<tr style="text-align: center">')

                            for index, row in unmatched_rows_sql_2.iterrows():
                                for col, value in row.items():
                                    
                                    if col in primary_keys_sql_2:
                                                unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                                    elif ( value != '-' and col not in primary_keys_sql_2):
                                                unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)


                            html_reports.append({
                                'table_name1': table_name1,
                                'table_name2': table_name2,
                                'comparison_result': comparison,
                                'unmatched_rows_source_html': unmatched_rows_sql_1_html,
                                'unmatched_rows_target_html': unmatched_rows_sql_2_html,
                                'total_rows_source': total_rows_sql_1,
                                'total_rows_target': total_rows_sql_2,
                                'count_unmatched_source': count_unmatched_sql_1,
                                'count_unmatched_target': count_unmatched_sql_2,
                    
                                'compare_url': url_for('compare', table1=table_name1, table2=table_name2)  # Pass table names as parameters

                            })

                    if not found_match:
                        unmatched_reports.append({
                        'schema1_name': schema1_name,
                        'table_name1': table_name1,
                        'Key_Matching': url_for('Key_Matching', schema1_name=schema1_name, table_name1=table_name1)
                    })
              



                context = {'reports': html_reports, 
                            'schema1_name': schema1_name,
                            'schema2_name': schema2_name,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                            }
                
                return render_template('schema.html', **context)

        elif(database=="synapse"):
            if (file_input == "synapse" ):
                
                print(schema2)
                
                print(schema1)
                
                print(boolean_input_value)
                
                if boolean_input_value == 'yes':
                    table_names=True
                else:
                    table_names=False
                
                
                
                def connection(data):
                    connection_dict = {}
                    for index, row in data.iterrows():
                        connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']

                    for key in connection_dict:
                        if key == "server":
                            server = connection_dict[key]
                        elif key =="database":
                            database = connection_dict[key]
                        elif key =="username":
                            username = connection_dict[key]
                    driver= '{ODBC Driver 17 for SQL Server}'

                    connection = (
                        f'DRIVER={driver};'
                        f'SERVER={server};'
                        f'DATABASE={database};'
                        f'Trusted_Connection=no;'
                        f'Authentication=ActiveDirectoryInteractive;'
                        f'MFA=Required;'
                        f'UID={username};'
                    )
                    return connection
                data_1 = pd.read_excel('C:\TNDV\Schema\Synapse_Connection_TableNames.xlsx', sheet_name='connection_1')
                data_2 = pd.read_excel('C:\TNDV\Schema\Synapse_Connection_TableNames.xlsx', sheet_name='connection_2')

                connection_1 = connection(data_1)
                connection_2 = connection(data_2)

                conn1 = pyodbc.connect(connection_1)
                conn2 = pyodbc.connect(connection_2)  
                if table_names:
                    table_names1=pd.read_excel('C:\TNDV\Schema\Synapse_Connection_TableNames.xlsx', sheet_name='tables_1')
                    table_names2=pd.read_excel('C:\TNDV\Schema\Synapse_Connection_TableNames.xlsx', sheet_name='tables_2')
                    queries_1=table_names1['Query'].tolist()
                    unique_keys1=table_names1['Unique_Key'].tolist()
                    table_names1= table_names1['Table_Names'].tolist()
                    queries_2=table_names2['Query'].tolist()
                    unique_keys2=table_names2['Unique_Key'].tolist()
                    table_names2= table_names2['Table_Names'].tolist()
                    print(table_names1)
                    print(table_names2)
                
                else:
                    sql_query = """SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{}'"""
                    sql_query1=sql_query.format(schema1)
                    cursor1 = conn1.cursor()
                    cursor1.execute(sql_query1)
                    table_names1 = cursor1.fetchall()
                    cursor1.close()
                    
                    
                    sql_query2=sql_query.format(schema2)
                    cursor2 = conn2.cursor()
                    cursor2.execute(sql_query2)
                    table_names2 = cursor2.fetchall()
                    cursor2.close()
                    
                    tables=[]
                    for i in table_names1:
                        for j in table_names2:
                            if i==j:
                                tables.append(i)
                    
                    
                    extracted_table_names = [name[0] for name in tables]
                    table_names1=[schema1+'.'+ name for name in extracted_table_names]
                    table_names2=[schema2+'.'+ name for name in extracted_table_names]
                    queries_1=[np.nan for _ in range(len(table_names1))]
                    queries_2=[np.nan for _ in range(len(table_names1))]
                    print(table_names1)
                    print(table_names2)
    
                html_reports=[]
                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0
                count_tables_source=len(table_names1)
                count_tables_target=len(table_names2)
                for table1,table2,query1,query2,unique_key1,unique_key2 in zip(table_names1,table_names2,queries_1,queries_2,unique_keys1,unique_keys2):
                    if isinstance(query1, str):
                        query1 = query1.replace(u'\xa0', ' ')
                        query1 = query1.format(table_name1=table1)
                    elif isinstance(unique_key1, str):
                        query1="select top 100 * from {} order by {}".format(table1,unique_key1)
                    elif math.isnan(query1):  # Check if query1 is a float NaN
                        query1 = "select * from {} order by rowindex".format(table1)
                    print("*******************************************************")
                    print(query1)
                    print("################################################################")
                    if isinstance(query2, str):
                        query2 = query2.replace(u'\xa0', ' ')
                        query2 = query2.format(table_name2=table2)
                    elif isinstance(unique_key1, str):
                        query2="select top 100 * from {} order by {}".format(table2,unique_key2)
                    elif math.isnan(query2):  # Check if query1 is a float NaN
                        query2 = "select * from {} order by rowindex".format(table2)
                    
                    
                    print(query2)
                    df1=pd.read_sql(query1,conn1)
                    df2=pd.read_sql(query2,conn2)
                   
                    columns_selected=[]
                    
                    
                    for column_name in df1:
                        if column_name in df2:
                            a=['RowIndex','pipeline_runtime_id','AsAtDate','PipelineKey']
                            if column_name not in a:
                                columns_selected.append(column_name)
                    print(columns_selected) 
                    df1 = df1.replace(['nan', 'None', 'Null', 'NaN','NUL','NULL', ''], '')
                                    
                    df2= df2.replace(['nan', 'None', 'Null','NUL' ,'NaN','NULL', ''], '')
                    df1= df1.fillna(value='')
                    df2 = df2.fillna(value='')
                                    
                    df1 =df1[columns_selected]
                    df2 = df2[columns_selected]
                    
                    
                   
                    
                        
                        
                    print("-----------------------------------------------")
                    print(df1)
                    print("===================================================")
                    print(df2)
                    print("++++++++++++++++++++++++++++++++++++++++++++")
                    df_sql_1 = df1.astype(str)
                    df_sql_2 = df2.astype(str)

                    comparison = df_sql_1.equals(df_sql_2)
    
                    unmatched_rows_sql_1 = df_sql_1[~df_sql_1.isin(df_sql_2)].dropna(how='all').fillna("-")
                    unmatched_rows_sql_1.index = unmatched_rows_sql_1.index + 1  
                    print(unmatched_rows_sql_1)

                    unmatched_rows_sql_2 = df_sql_2[~df_sql_2.isin(df_sql_1)].dropna(how='all').fillna("-")
                    unmatched_rows_sql_2.index = unmatched_rows_sql_2.index +1
                    print(unmatched_rows_sql_2)

  
                    total_rows_sql_1= len(df_sql_1)
                    total_rows_sql_2 = len(df_sql_2)
                    count_unmatched_df1 = 0
                    count_unmatched_df2 = 0
                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        count_unmatched_df1 =len(unmatched_rows_sql_1.index)
                        count_unmatched_df2=len(unmatched_rows_sql_2.index)
                    if count_unmatched_df1 >1000:
                        unmatched_rows_sql_1=unmatched_rows_sql_1.iloc[0:1000]
                    if count_unmatched_df2>1000:
                        unmatched_rows_sql_2=unmatched_rows_sql_2.iloc[0:1000]
                    unmatched_rows_df1_html = unmatched_rows_sql_1.to_html(na_rep='-', escape=False)
                    unmatched_rows_df1_html = unmatched_rows_df1_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_df1_html = unmatched_rows_df1_html.replace('<tr>', '<tr style="text-align: center">')
                    for index, row in unmatched_rows_sql_1.iterrows():
                        for col, value in row.items():
                            if ( value != '-'):
                                unmatched_rows_df1_html = unmatched_rows_df1_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)
                    unmatched_rows_df2_html = unmatched_rows_sql_2.to_html(na_rep='-', escape=False)
                    unmatched_rows_df2_html = unmatched_rows_df2_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_df2_html = unmatched_rows_df2_html.replace('<tr>', '<tr style="text-align: center">')
                    for index, row in unmatched_rows_sql_2.iterrows():
                        for col, value in row.items():
                            if ( value != '-'):
                                unmatched_rows_df2_html = unmatched_rows_df2_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)
                    
                    html_reports.append({
                        'table_name1': table1,
                        'table_name2': table2,
                        'comparison_result': comparison,
                        'unmatched_rows_source_html': unmatched_rows_df1_html,
                        'unmatched_rows_target_html': unmatched_rows_df2_html,
                        'total_rows_source': total_rows_sql_1,
                        'total_rows_target': total_rows_sql_2,
                        'count_unmatched_source': count_unmatched_df1,
                        'count_unmatched_target': count_unmatched_df2,
                        'compare_url': url_for('compare', table1=table1, table2=table2),
                        'compare_pass_url':url_for('compare_pass',table1=table1, table2=table2)
      


                    }) 
                    
                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                        }
                return render_template('Multiple.html', **context)
                # context = {'reports': html_reports, 
                #             'schema1_name': schema1_name,
                #             'schema2_name': schema2_name,
                #             'count_tables_source': count_tables_source,
                #             'count_tables_target': count_tables_target,
                #             'count_unmatched_tables_source': count_unmatched_tables_source,
                #             'count_unmatched_tables_target': count_unmatched_tables_target
                #             }
                
                # return render_template('schema.html', **context)            

         

        
        
        
        else:

            schema1_name = file_name
            schema2_name = table
            data_1 = pd.read_excel('C:\TNDV\Schema\Snowflake_Connection.xlsx',sheet_name='Snowflake_Connection-1')

            connection_dict = {}
            for index, row in data_1.iterrows():
                connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
            print(connection_dict)
            for key in connection_dict:
                print(key)
                if key == "account":
                    account1 = connection_dict[key]
                    print(account1)
                elif key =="warehouse":
                    warehouse1 = connection_dict[key]
                elif key =="database":
                    database1 = connection_dict[key]
                elif key =="username":
                    username1 = connection_dict[key]
                password1 =password

            # ************************************************************************************
            if(file_input == "Snowflake"):
                data_2 = pd.read_excel('C:\TNDV\Schema\Snowflake_Connection.xlsx',sheet_name='Snowflake_Connection-2')

                connection_dict = {}
                for index, row in data_2.iterrows():
                    connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
                print(connection_dict)
                for key in connection_dict:
                    print(key)
                    if key == "account":
                        account2 = connection_dict[key]
                        print(account2)
                    elif key =="warehouse":
                        warehouse2 = connection_dict[key]
                    elif key =="database":
                        database2 = connection_dict[key]
                    
                    elif key =="username":
                        username2 = connection_dict[key]

                password2 = source_password

                conn1 = snowflake.connector.connect(
                    account=account1,
                    warehouse=warehouse1,
                    database=database1,
                    schema=schema1_name,
                    user=username1,
                    password=password1
                )

                conn2 = snowflake.connector.connect(
                    account=account2,
                    warehouse=warehouse2,
                    database=database2,
                    schema=schema2_name,
                    user=username2,
                    password=password2
                )


                def get_primary_keys_sf(connection, schema, table_name):
                    cursor = connection.cursor()
                    query = f"""
                        SHOW PRIMARY KEYS IN TABLE {schema}.{table_name}
                    """
                    cursor.execute(query)
                    primary_keys = [row[4] for row in cursor.fetchall()]
                    return primary_keys

            

                def get_table_names(connection, schema):
                    query = f"""
                        SHOW TABLES IN SCHEMA {schema}
                    """
                    tables_df = pd.read_sql(query, connection)
                    return tables_df['name'].tolist()

                # Get list of table names in both schemas
                table_names1 = get_table_names(conn1, schema1_name)
                table_names2 = get_table_names(conn2, schema2_name)

                count_tables_source = len(table_names1)
                count_tables_target = len(table_names2)


                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0

                for table_name1 in table_names1:
                    primary_keys_sql_1 = get_primary_keys_sf(conn1, schema1_name, table_name1)
             

                    found_match = False

                    for table_name2 in table_names2:
                        primary_keys_sql_2 = get_primary_keys_sf(conn2, schema2_name, table_name2)
                      

                        if primary_keys_sql_1 == primary_keys_sql_2:

                            found_match = True

                            query1 = f"SELECT * FROM {schema1_name}.{table_name1}"
                            query2 = f"SELECT * FROM {schema2_name}.{table_name2}"

                            print(f"Comparing {schema1_name}.{table_name1} and {schema2_name}.{table_name2}")

                            df_sql_1 = pd.read_sql(query1, conn1)
                            df_sql_2 = pd.read_sql(query2, conn2)


                            df_sql_1 = df_sql_1.astype(str)
                            df_sql_2 = df_sql_2.astype(str)

                            comparison = df_sql_1.equals(df_sql_2)


                            unmatched_rows_sql_1 = df_sql_1[~df_sql_1.isin(df_sql_2)].dropna(how='all')
                            for col in primary_keys_sql_1:
                                unmatched_rows_sql_1[col] = df_sql_1[col]
                            unmatched_rows_sql_1.index = unmatched_rows_sql_1.index + 1  
                            unmatched_rows_sql_1 = unmatched_rows_sql_1[~unmatched_rows_sql_1.isin(primary_keys_sql_1)].dropna(how='all')

                          
                            unmatched_rows_sql_2 = df_sql_2[~df_sql_2.isin(df_sql_1)].dropna(how='all')
                            for col in primary_keys_sql_2:
                                unmatched_rows_sql_2[col] = df_sql_2[col]
                            unmatched_rows_sql_2.index = unmatched_rows_sql_2.index + 1  
                            unmatched_rows_sql_2 = unmatched_rows_sql_2[~unmatched_rows_sql_2.isin(primary_keys_sql_2)].dropna(how='all')



                            total_rows_sql_1 = len(df_sql_1)
                            total_rows_sql_2 = len(df_sql_2)

                            count_unmatched_sql_1 = 0
                            count_unmatched_sql_2 = 0


                            if not comparison:
                                count_unmatched_tables_source += 1
                                count_unmatched_tables_target += 1

                                count_unmatched_sql_1 =len(unmatched_rows_sql_1.index)
                                count_unmatched_sql_2 =len(unmatched_rows_sql_2.index)

                            unmatched_rows_sql_1_html = unmatched_rows_sql_1.to_html(na_rep='-', escape=False)
                            unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                            unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace('<tr>', '<tr style="text-align: center">')

                        

                            for index, row in unmatched_rows_sql_1.iterrows():
                                for col, value in row.items():
                                    
                                    if col in primary_keys_sql_1:
                                                unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                                    elif ( value != '-' and col not in primary_keys_sql_1):
                                                unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

                        





                            # Convert unmatched_rows_sql_2 to HTML with background colors
                            unmatched_rows_sql_2_html = unmatched_rows_sql_2.to_html(na_rep='-', escape=False)
                            unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                            unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace('<tr>', '<tr style="text-align: center">')

                            for index, row in unmatched_rows_sql_2.iterrows():
                                for col, value in row.items():
                                    
                                    if col in primary_keys_sql_2:
                                                unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                                    elif ( value != '-' and col not in primary_keys_sql_2):
                                                unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)





                            html_reports.append({
                                'table_name1': table_name1,
                                'table_name2': table_name2,
                                'comparison_result': comparison,
                                'unmatched_rows_source_html': unmatched_rows_sql_1_html,
                                'unmatched_rows_target_html': unmatched_rows_sql_2_html,
                                'total_rows_source': total_rows_sql_1,
                                'total_rows_target': total_rows_sql_2,
                                'count_unmatched_source': count_unmatched_sql_1,
                                'count_unmatched_target': count_unmatched_sql_2,
                    
                                'compare_url': url_for('compare', table1=table_name1, table2=table_name2)  # Pass table names as parameters
                            
                        

                        })
                            break
                        
                    
                    if not found_match:
                        unmatched_reports.append({
                        'schema1_name': schema1_name,
                        'table_name1': table_name1,
                        'Key_Matching': url_for('Key_Matching', schema1_name=schema1_name, table_name1=table_name1)
                    })
                       


                context = {'reports': html_reports, 
                            'schema1_name': schema1_name,
                            'schema2_name': schema2_name,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                            }
               
                return render_template('schema.html', **context)
            
    

    elif(validation_type =="many_tables"):
        print("=====================================================================")

        if (database == "Snowflake"):

            print("ppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp")
               
            data = pd.read_excel("C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx",sheet_name='Connection')
            print(data)
            connection_dict = {}
            for index, row in data.iterrows():
                connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
            print(connection_dict)
            for key in connection_dict:
                print(key)
                if key == "account":
                    account = connection_dict[key]
                    print(account)
                elif key =="warehouse":
                    warehouse = connection_dict[key]
                elif key =="database":
                    database = connection_dict[key]
                elif key =="schema":
                    schema = connection_dict[key]
                    print(schema)
                elif key =="username":
                    username = connection_dict[key]
        
            password_sw = password
           
            conn = snowflake.connector.connect(
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
                user=username,
                password=password_sw
            )



            def get_primary_keys_snowflake(connection, table_name):
                cursor = connection.cursor()
                query = f"""
                    SHOW PRIMARY KEYS IN TABLE {table_name}
                """
                cursor.execute(query)
                primary_keys = [row[4] for row in cursor.fetchall()]
                
                return primary_keys
        
            
             ###############-----------------SNOWFLAKE VS json--------------------###############################
            if (file_input== "json"):

                print("+++++++++++++++++++++++++++++++++++++++++")
                json_files_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx', sheet_name=file_name)
               

                json_files = json_files_df['File_Names'].tolist()


                csv_file_path = "C:\\TNDV\\Multiple_Tables\\Folder_Path_MultipleTables.csv"
                df_csv = pd.read_csv(csv_file_path)
            
                # Loop through each row in the CSV
                for index, row in df_csv.iterrows():
                    json_folder = row['json_path'].strip('"')    # Assuming 'json_path' is the column containing the JSON path
                    print(json_folder,"^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                 

                # Read table names from CSV
                table_names_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx', sheet_name=table)
                table_names = table_names_df['Table_Names'].tolist()
          

                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0

                for json_file_input, table_name2 in zip(json_files, table_names):
                    table_name1 = json_file_input + '.json'
                    json_file = os.path.join(json_folder,table_name1 )
                    print(json_file,"&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")

                    data_dict = pd.read_json(json_file)
                    df_json = pd.DataFrame(data_dict)
                    df_json.columns = df_json.columns.str.upper()

                    query = f"SELECT * FROM {table_name2}"
                    df_snowflake = pd.read_sql(query, conn)

                    primary_keys_table_snowflake = get_primary_keys_snowflake(conn,table_name2)

                    # count_tables_source = len(table_name1)
                    # tables = df_snowflake['TABLE_NAME'].tolist()
                    # count_tables_target = len(tables)

                    count_tables_source = len(json_files)
                    count_tables_target= len(table_names)




                    def get_column_data_types():
                        column_data_types = {}
                        query = f"SHOW COLUMNS IN TABLE {table_name2}"
                        cursor = conn.cursor()
                        cursor.execute(query)
                    # Loop through the result and extract column names and data types
                        for row in cursor:
                            # print(row)
                            column_name = row[2]  # Column name is in the second position
                            data_type = row[3]    # Data type is in the third position
                            dict_data = json.loads(data_type)
                            for data in dict_data:
                                data=dict_data['type']
                            column_data_types[column_name] = data
                            
                        return column_data_types

                    column_data_types_dict = get_column_data_types()
                    # print(column_data_types_dict)
                    df_snowflake = df_snowflake[df_json.columns]
                    columns_selected=[]
                    dict_selected ={}

                    for key in column_data_types_dict:
                        if key in df_json.columns:
                            columns_selected.append(key)

                    for key in column_data_types_dict:
                        if key in columns_selected:
                            if (column_data_types_dict[key]== 'TIMESTAMP_NTZ'):
                                # print(key)
                                df_json[key] = pd.to_datetime(df_json[key])
                                df_json[key] = df_json[key].dt.strftime('%Y-%m-%d %H:%M')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d %H:%M')
                                
                            elif (column_data_types_dict[key]=="DATE"):
                                df_json[key] = pd.to_datetime(df_json[key])
                                df_json[key] = df_json[key].dt.strftime('%Y-%m-%d')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d')
     
                            elif (column_data_types_dict[key]== 'BOOLEAN'):
                                df_snowflake[key] = df_snowflake[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                                df_json[key] = df_json[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                            elif (column_data_types_dict[key]== 'TEXT'):
                           
                                if df_json[key].dtype == 'float64' or df_json[key].dtype == 'int64' :
                                    df_json[key] = df_json[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)

                    #null-values and str type conversion
                    df_json = df_json.fillna(value='Null')
                    df_snowflake = df_snowflake.fillna(value='Null')


                    df_json = df_json.astype(str)
                    df_snowflake = df_snowflake.astype(str)
          
                    comparison = df_json.equals(df_snowflake)

                

                    unmatched_rows_snowflake = df_snowflake[~df_snowflake.isin(df_json)].dropna(how='all')
                    for col in primary_keys_table_snowflake:
                        unmatched_rows_snowflake[col] = df_snowflake[col]
                    unmatched_rows_snowflake.index = unmatched_rows_snowflake.index + 1  
                    unmatched_rows_snowflake = unmatched_rows_snowflake[~unmatched_rows_snowflake.isin(primary_keys_table_snowflake)].dropna(how='all')
                    

                    primary_keys_file  =  [col for col in primary_keys_table_snowflake if col in df_json.columns]
                    if primary_keys_file:
                        unmatched_rows_json = df_json[~df_json.isin(df_snowflake)].dropna(how='all')
                
                        for col in primary_keys_file:
                            unmatched_rows_json[col] = df_json[col]  # Perform operations on the specific column
                        unmatched_rows_json.index = unmatched_rows_json.index + 1  
                        unmatched_rows_json = unmatched_rows_json[~unmatched_rows_json.isin(primary_keys_file)].dropna(how='all')


                    total_rows_json = len(df_json)
                    total_rows_snowflake = len(df_snowflake)

                    count_unmatched_json = 0
                    count_unmatched_snowflake = 0

                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        count_unmatched_json =len(unmatched_rows_json.index)
                        count_unmatched_snowflake =len(unmatched_rows_snowflake.index)




                    # Set background color for unmatched rows to red and remaining cells to green
                    unmatched_rows_json_html = unmatched_rows_json.to_html(na_rep='-', escape=False)
                    unmatched_rows_json_html = unmatched_rows_json_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_json_html = unmatched_rows_json_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_json.iterrows():
                        for col, value in row.items():
                            if col in primary_keys_file:
                                unmatched_rows_json_html = unmatched_rows_json_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                            if ( value != '-'and col not in primary_keys_file):
                                unmatched_rows_json_html = unmatched_rows_json_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

                    

                    unmatched_rows_snowflake_html =  unmatched_rows_snowflake.to_html(na_rep='-', escape=False)
                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_snowflake.iterrows():
                        for col, value in row.items():
                            
                            if col in primary_keys_table_snowflake:
                                        unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                            elif ( value != '-' and col not in primary_keys_table_snowflake):
                                        unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)



                    html_reports.append({
                        'table_name1': table_name1,
                        'table_name2': table_name2,
                        'comparison_result': comparison,
                        'unmatched_rows_source_html': unmatched_rows_json_html,
                        'unmatched_rows_target_html': unmatched_rows_snowflake_html,
                        'total_rows_source': total_rows_json,
                        'total_rows_target': total_rows_snowflake,
                        'count_unmatched_source': count_unmatched_json,
                        'count_unmatched_target': count_unmatched_snowflake,
                        'compare_url': url_for('compare', table1=table_name1, table2=table_name2)






                    })

                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                        }
               
                
                return render_template('Multiple.html', **context)
                
            
                

                
            ###############-----------------SNOWFLAKE VS Flatfile --------------------###############################
                

            elif (file_input== "FlatFile"):

                csv_files_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx', sheet_name=file_name)
                print(csv_files_df)
                csv_files = csv_files_df['File_Names'].tolist()
                print(csv_files,"***********************************")






                csv_file_path = "C:\\TNDV\\Multiple_Tables\\Folder_Path_MultipleTables.csv"
                df_csv = pd.read_csv(csv_file_path)
                
                # Loop through each row in the CSV
                for index, row in df_csv.iterrows():
                    #json_path1 = row['json_path']  # Assuming 'json_path' is the column containing the JSON path
                    csv_folder = row['flat_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path
                    

                # Read table names from CSV
                # table_names_csv =  r"C:\\TNDV\\Multiple_Tables\\Multiple\\Table_Names.csv" 
                table_names_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx', sheet_name=table)
                print(table_names_df)
                table_names = table_names_df['Table_Names'].tolist()



    

                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0

                for csv_file_input, table_name2 in zip(csv_files, table_names):
                    table_name1 = csv_file_input + '.txt'
                    csv_file = os.path.join(csv_folder,table_name1)

                    #determining the delimiter of the flat table_name1
                    def determine_delimiter(file_path, num_lines=5):
                        comma_count = 0
                        tab_count = 0
                        pipe_count = 0


                        with open(file_path, 'r') as file:
                            for _ in range(num_lines):
                                line = file.readline()
                                comma_count += line.count(',')
                                tab_count += line.count('\t')
                                pipe_count +=line.count('|')

                        if comma_count > tab_count and comma_count > pipe_count:
                            return ','
                        elif tab_count > comma_count and tab_count > pipe_count:
                            return '\t'
                        elif pipe_count > comma_count and pipe_count > tab_count:
                            return '|'
                        else:
                            return None  # Use the default delimiter


                    # Fetch data from Snowflake table
                    query = f"SELECT * FROM {table_name2}"
                    df_snowflake = pd.read_sql(query, conn)

                    primary_keys_table_snowflake = get_primary_keys_snowflake(conn,table_name2)

                    count_tables_source = len(csv_files)
                    count_tables_target= len(table_names)


                    def get_column_data_types():
                        column_data_types = {}
                        query = f"SHOW COLUMNS IN TABLE {table_name2}"
                        cursor = conn.cursor()
                        cursor.execute(query)
                        # Loop through the result and extract column names and data types
                        for row in cursor:

                            column_name = row[2]  # Column name is in the second position
                            data_type = row[3]    # Data type is in the third position

                            dict_data = json.loads(data_type)
                            for data in dict_data:
                                data=dict_data['type']
                            column_data_types[column_name] = data

                        return column_data_types

                    column_data_types_dict = get_column_data_types()
                    separator = determine_delimiter(csv_file)

                    # Read the flat file into a DataFrame with the determined delimiter
                    if separator:
                        df_csv = pd.read_csv(csv_file, delimiter=separator)
                    else:
                        raise ValueError("Could not determine the delimiter of the flat file")

                    df_csv.columns = df_csv.columns.str.upper()
                    columns_selected =[]

                    for column_name in df_csv.columns:
                        if column_name in column_data_types_dict:
                            columns_selected.append(column_name)


                    df_snowflake = df_snowflake[columns_selected]
                    df_csv = df_csv[columns_selected]

                    for key in column_data_types_dict:
                        if key in columns_selected:
                            if (column_data_types_dict[key]== 'TIMESTAMP_NTZ'):
                                
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d %H:%M')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d %H:%M')
                            
                            elif (column_data_types_dict[key]=="DATE"):
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d')

                            elif (column_data_types_dict[key]=="DATE"):
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d')


                            elif (column_data_types_dict[key]== 'BOOLEAN'):
                                df_snowflake[key] = df_snowflake[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                                df_csv[key] = df_csv[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                            elif (column_data_types_dict[key]== 'TEXT'):
                                if df_csv[key].dtype == 'float64' or df_csv[key].dtype == 'int64' :
                                    df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)


                    df_csv = df_csv.fillna(value='Null')
                    df_snowflake = df_snowflake.fillna(value='Null')
                    df_snowflake = df_snowflake[columns_selected]
                    df_csv = df_csv[columns_selected]
                    # print(df_snowflake,"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")


                    df_csv = df_csv.astype(str)
                    df_snowflake = df_snowflake.astype(str)
                    comparison = df_csv.equals(df_snowflake)


                    unmatched_rows_snowflake = df_snowflake[~df_snowflake.isin(df_csv)].dropna(how='all')
                    for col in primary_keys_table_snowflake:
                        unmatched_rows_snowflake[col] = df_snowflake[col]
                        print(df_snowflake.columns,"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                    unmatched_rows_snowflake.index = unmatched_rows_snowflake.index + 1  
                    unmatched_rows_snowflake = unmatched_rows_snowflake[~unmatched_rows_snowflake.isin(primary_keys_table_snowflake)].dropna(how='all')
                    

                    primary_keys_file  =  [col for col in primary_keys_table_snowflake if col in df_csv.columns]
                    if primary_keys_file:
                        unmatched_rows_csv = df_csv[~df_csv.isin(df_snowflake)].dropna(how='all')
                
                        for col in primary_keys_file:
                            unmatched_rows_csv[col] = df_csv[col]  # Perform operations on the specific column
                        unmatched_rows_csv.index = unmatched_rows_csv.index +1
                        unmatched_rows_csv = unmatched_rows_csv[~unmatched_rows_csv.isin(primary_keys_file)].dropna(how='all')


                    total_rows_csv = len(df_csv)
                    total_rows_snowflake = len(df_snowflake)


                    count_unmatched_csv = 0
                    count_unmatched_snowflake = 0

                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        count_unmatched_csv =len(unmatched_rows_csv.index)
                        count_unmatched_snowflake =len(unmatched_rows_snowflake.index)

                    # Set background color for unmatched rows to red and remaining cells to green
                    unmatched_rows_csv_html = unmatched_rows_csv.to_html(na_rep='-', escape=False)
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_csv.iterrows():
                        for col, value in row.items():
                            if col in primary_keys_file:
                                unmatched_rows_csv_html = unmatched_rows_csv_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                            if ( value != '-'):
                                unmatched_rows_csv_html = unmatched_rows_csv_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

                    

                    unmatched_rows_snowflake_html =  unmatched_rows_snowflake.to_html(na_rep='-', escape=False)
                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_snowflake.iterrows():
                        for col, value in row.items():
                            
                            if col in primary_keys_table_snowflake:
                                        unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                            elif ( value != '-' and col not in primary_keys_table_snowflake):
                                        unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

                    html_reports.append({
                        'table_name1': table_name1,
                        'table_name2': table_name2,
                        'comparison_result': comparison,
                        'unmatched_rows_source_html': unmatched_rows_csv_html,
                        'unmatched_rows_target_html': unmatched_rows_snowflake_html,
                        'total_rows_source': total_rows_csv,
                        'total_rows_target': total_rows_snowflake,
                        'count_unmatched_source': count_unmatched_csv,
                        'count_unmatched_target': count_unmatched_snowflake,
                        'compare_url': url_for('compare', table1=table_name1, table2=table_name2)  # Pass table names as parameters
                        







                    })

                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                        }
                
                return render_template('Multiple.html', **context)
            

                
            ###############-----------------SNOWFLAKE VS csv --------------------###############################



            elif (file_input == "csv"):

                csv_files_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx', sheet_name=file_name)
                print(csv_files_df)
                csv_files = csv_files_df['File_Names'].tolist()



                csv_file_path = "C:\\TNDV\\Multiple_Tables\\Folder_Path_MultipleTables.csv"
                df_csv = pd.read_csv(csv_file_path)
                
                # Loop through each row in the CSV
                for index, row in df_csv.iterrows():
                   
                   csv_folder= row['csv_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path


              
                table_names_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx',sheet_name=table)
                print(table_names_df)
                table_names = table_names_df['Table_Names'].tolist()

                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0

                for csv_file_input, table_name2 in zip(csv_files, table_names):
                    table_name1 = csv_file_input + '.csv'
                    csv_file = os.path.join(csv_folder,table_name1)

                    
                    df_csv = pd.read_csv(csv_file)
                    # df_csv.columns = df_csv.columns.str.upper()

                    query = f"SELECT * FROM {table_name2}"
                    df_snowflake = pd.read_sql(query, conn)

                    primary_keys_table_snowflake = get_primary_keys_snowflake(conn,table_name2)

                    count_tables_source = len(csv_files)
                    count_tables_target= len(table_names)

                    # ... (rest of the code for data comparison)
                
                    def get_column_data_types():
                        column_data_types = {}
                        query = f"SHOW COLUMNS IN TABLE {table_name2}"
                        cursor = conn.cursor()
                        cursor.execute(query)
                    # Loop through the result and extract column names and data types
                        for row in cursor:
                            # print(row)
                            column_name = row[2]  # Column name is in the second position
                            data_type = row[3]    # Data type is in the third position
                       
                            dict_data = json.loads(data_type)
                            for data in dict_data:
                                data=dict_data['type']
                            column_data_types[column_name] = data
                            
                        return column_data_types

                    column_data_types_dict = get_column_data_types()

                    df_csv.columns = df_csv.columns.str.upper()
                    columns_selected =[]

                    for column_name in df_csv.columns:
                        if column_name in column_data_types_dict:
                            columns_selected.append(column_name)

                    df_snowflake = df_snowflake[df_csv.columns]
                    df_snowflake = df_snowflake[columns_selected]
                    df_csv = df_csv[columns_selected]
 


                    for key in column_data_types_dict:
                        if key in columns_selected:
                            if (column_data_types_dict[key]== 'TIMESTAMP_NTZ'):
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d %H:%M')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d %H:%M')
                                

                            elif (column_data_types_dict[key]=="DATE"):
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d')

                                
                            elif (column_data_types_dict[key]== 'BOOLEAN'):
                                df_snowflake[key] = df_snowflake[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                                df_csv[key] = df_csv[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                            elif (column_data_types_dict[key]== 'TEXT'):
    
                                if df_csv[key].dtype == 'float64' or df_csv[key].dtype == 'int64' :
                                    df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)

                    #null-values and str type conversion
                    df_csv = df_csv.fillna(value='Null')
                    df_snowflake = df_snowflake.fillna(value='Null')
                    df_snowflake = df_snowflake[columns_selected]
                    df_csv = df_csv[columns_selected]


                    df_csv = df_csv.astype(str)
                    df_snowflake = df_snowflake.astype(str)
            
                    comparison = df_csv.equals(df_snowflake)

                    unmatched_rows_snowflake = df_snowflake[~df_snowflake.isin(df_csv)].dropna(how='all')
                    for col in primary_keys_table_snowflake:
                        print(df_snowflake.columns,"*********************************************************")
                        unmatched_rows_snowflake[col] = df_snowflake[col]

                    unmatched_rows_snowflake.index = unmatched_rows_snowflake.index + 1  
                    unmatched_rows_snowflake = unmatched_rows_snowflake[~unmatched_rows_snowflake.isin(primary_keys_table_snowflake)].dropna(how='all')
                    

                    primary_keys_file  =  [col for col in primary_keys_table_snowflake if col in df_csv.columns]
                    if primary_keys_file:
                        unmatched_rows_csv = df_csv[~df_csv.isin(df_snowflake)].dropna(how='all')
                
                        for col in primary_keys_file:
                            unmatched_rows_csv[col] = df_csv[col]  # Perform operations on the specific column
                        unmatched_rows_csv.index = unmatched_rows_csv.index +1
                        unmatched_rows_csv = unmatched_rows_csv[~unmatched_rows_csv.isin(primary_keys_file)].dropna(how='all')

                    total_rows_csv = len(df_csv)
                    total_rows_snowflake = len(df_snowflake)

                    count_unmatched_csv = 0
                    count_unmatched_snowflake = 0

                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        count_unmatched_csv =len(unmatched_rows_csv.index)
                        count_unmatched_snowflake =len(unmatched_rows_snowflake.index)

                    # Set background color for unmatched rows to red and remaining cells to green
                    unmatched_rows_csv_html = unmatched_rows_csv.to_html(na_rep='-', escape=False)
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_csv.iterrows():
                        for col, value in row.items():
                            if col in primary_keys_file:
                                unmatched_rows_csv_html = unmatched_rows_csv_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                            if ( value != '-'and col not in primary_keys_file):
                                unmatched_rows_csv_html = unmatched_rows_csv_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

                    

                    unmatched_rows_snowflake_html =  unmatched_rows_snowflake.to_html(na_rep='-', escape=False)
                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_snowflake.iterrows():
                        for col, value in row.items():
                            
                            if col in primary_keys_table_snowflake:
                                        unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                            elif ( value != '-' and col not in primary_keys_table_snowflake):
                                        unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)


                    html_reports.append({
                        'table_name1': table_name1,
                        'table_name2': table_name2,
                        'comparison_result': comparison,
                        'unmatched_rows_source_html': unmatched_rows_csv_html,
                        'unmatched_rows_target_html': unmatched_rows_snowflake_html,
                        'total_rows_source': total_rows_csv,
                        'total_rows_target': total_rows_snowflake,
                        'count_unmatched_source': count_unmatched_csv,
                        'count_unmatched_target': count_unmatched_snowflake,
                        'compare_url': url_for('compare', table1=table_name1, table2=table_name2)
      


                    })

                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                        }
              
                return render_template('Multiple.html', **context)
             ###############-----------------SNOWFLAKE VS xml --------------------###############################



            elif (file_input == "xml"):

                xml_files_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx', sheet_name=file_name)
                print(xml_files_df)
                xml_files = xml_files_df['File_Names'].tolist()
                print(xml_files,"***********************************")


                csv_file_path = "C:\\TNDV\\Multiple_Tables\\Folder_Path_MultipleTables.csv"

                df_csv = pd.read_csv(csv_file_path)
                # Loop through each row in the CSV

                for index, row in df_csv.iterrows():

                    xml_folder = row['xml_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path


            
                table_names_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx', sheet_name=table)
                print(table_names_df)
                table_names = table_names_df['Table_Names'].tolist()

                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0

                for xml_file_input, table_name2 in zip(xml_files, table_names):
                    table_name1 = xml_file_input + '.xml'
                    xml_file = os.path.join(xml_folder,table_name1 )

                    
                    tree = ET.parse(xml_file)
                    root = tree.getroot()
                    xml_data = []
                    for child in root:
                        row = {}
                        for subchild in child:
                       
                            row[subchild.tag.upper()] = subchild.text
                        xml_data.append(row)
                    df_xml = pd.DataFrame(xml_data)

                    query = f"SELECT * FROM {table_name2}"
                    df_snowflake = pd.read_sql(query, conn)

                    primary_keys_table_snowflake = get_primary_keys_snowflake(conn,table_name2)


                    count_tables_source = len(xml_files)
                    count_tables_target= len(table_names)

                  
                    print("common_columns",df_snowflake)

                    def get_column_data_types():
                        column_data_types = {}
                        query = f"SHOW COLUMNS IN TABLE {table_name2}"
                        cursor = conn.cursor()
                        cursor.execute(query)
                    # Loop through the result and extract column names and data types
                        for row in cursor:
                            # print(row)
                            column_name = row[2]  # Column name is in the second position
                            data_type = row[3]    # Data type is in the third position
                    
                            dict_data = json.loads(data_type)
                            for data in dict_data:
                                data=dict_data['type']
                            column_data_types[column_name] = data
                            
                        return column_data_types

                    column_data_types_dict = get_column_data_types()
                


                    df_snowflake = df_snowflake[df_xml.columns]
                    columns_selected=[]
                    dict_selected ={}

                    for key in column_data_types_dict:
                        if key in df_xml.columns:
                            columns_selected.append(key)

                    for key in column_data_types_dict:
                        if key in columns_selected:
                            if (column_data_types_dict[key]== 'TIMESTAMP_NTZ'):
                                # print(key)
                                df_xml[key] = pd.to_datetime(df_xml[key])
                                df_xml[key] = df_xml[key].dt.strftime('%Y-%m-%d %H:%M')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d %H:%M')
                                
                            elif (column_data_types_dict[key]=="DATE"):
                                df_xml[key] = pd.to_datetime(df_xml[key])
                                df_xml[key] = df_xml[key].dt.strftime('%Y-%m-%d')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d')

                            elif (column_data_types_dict[key]== 'BOOLEAN'):
                                df_snowflake[key] = df_snowflake[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                                df_xml[key] = df_xml[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                            elif (column_data_types_dict[key]== 'TEXT'):
                              
                                if df_xml[key].dtype == 'float64' or df_xml[key].dtype == 'int64' :
                                    df_xml[key] = df_xml[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)

                    #null-values and str type conversion
                    df_xml = df_xml.fillna(value='Null')
                    df_snowflake = df_snowflake.fillna(value='Null')


                    df_xml = df_xml.astype(str)
                    df_snowflake = df_snowflake.astype(str)
              
                    comparison = df_xml.equals(df_snowflake)

                    if comparison:
                        print(f"The data in {table_name1} matches the data in the {table_name2} Snowflake table.")
                    else:
                        print(f"The data in {table_name1} does not match the data in the {table_name2} Snowflake table.")


                    unmatched_rows_snowflake = df_snowflake[~df_snowflake.isin(df_xml)].dropna(how='all')
                    for col in primary_keys_table_snowflake:
                        unmatched_rows_snowflake[col] = df_snowflake[col]
                    unmatched_rows_snowflake.index = unmatched_rows_snowflake.index + 1  
                    unmatched_rows_snowflake = unmatched_rows_snowflake[~unmatched_rows_snowflake.isin(primary_keys_table_snowflake)].dropna(how='all')
                    

                    primary_keys_file  =  [col for col in primary_keys_table_snowflake if col in df_xml.columns]
                    if primary_keys_file:
                        unmatched_rows_xml = df_xml[~df_xml.isin(df_snowflake)].dropna(how='all')
                
                        for col in primary_keys_file:
                            unmatched_rows_xml[col] = df_xml[col]  # Perform operations on the specific column
                        unmatched_rows_xml.index = unmatched_rows_xml.index + 1  
                        unmatched_rows_xml = unmatched_rows_xml[~unmatched_rows_xml.isin(primary_keys_file)].dropna(how='all')


                    total_rows_xml = len(df_xml)
                    total_rows_snowflake = len(df_snowflake)


                    count_unmatched_xml = 0
                    count_unmatched_snowflake = 0

                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        count_unmatched_xml =len(unmatched_rows_xml.index)
                        count_unmatched_snowflake =len(unmatched_rows_snowflake.index)

                     # Set background color for unmatched rows to red and remaining cells to green
                    unmatched_rows_xml_html = unmatched_rows_xml.to_html(na_rep='-', escape=False)
                    unmatched_rows_xml_html = unmatched_rows_xml_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_xml_html = unmatched_rows_xml_html.replace('<tr>', '<tr style="text-align: center">')

                    # Iterate over the DataFrame to set red background for non-matching cells
                    for index, row in unmatched_rows_xml.iterrows():
                        for col, value in row.items():
                
                            if ( value != '-' and col not in primary_keys_file):
                                        unmatched_rows_xml_html = unmatched_rows_xml_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)          
                            elif col in primary_keys_file:
                                        unmatched_rows_xml_html = unmatched_rows_xml_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)



                    unmatched_rows_snowflake_html = unmatched_rows_snowflake.to_html(na_rep='-', escape=False)
                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<tr>', '<tr style="text-align: center">')

                    # Iterate over the DataFrame to set red background for non-matching cells
                    for index, row in unmatched_rows_snowflake.iterrows():
                        for col, value in row.items():
                            
                            if ( value != '-' and col not in primary_keys_table_snowflake):
                                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)
                            elif col in primary_keys_table_snowflake:
                                        unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)

                    
                    html_reports.append({

                        'table_name1': table_name1,
                        'table_name2': table_name2,
                        'comparison_result': comparison,
                        'unmatched_rows_source_html': unmatched_rows_xml_html,
                        'unmatched_rows_target_html': unmatched_rows_snowflake_html,
                        'total_rows_source': total_rows_xml,
                        'total_rows_target': total_rows_snowflake,
                        'count_unmatched_source': count_unmatched_xml,
                        'count_unmatched_target': count_unmatched_snowflake,
                        'compare_url': url_for('compare', table1=table_name1, table2=table_name2)


                    })

                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                        }
               
               
                return render_template('Multiple.html', **context)

            ###############-----------------Snowflake vs AzureBlob csv--------------------###############################


            elif (file_input== "azure_blob_csv"):
                
                data = pd.read_excel("C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx",sheet_name='Blob_Connection')
                print(data)
                connection_dict = {}
                for index, row in data.iterrows():
                    connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
                print(connection_dict)
                for key in connection_dict:
                    print(key)
                    if key == "connection_string":
                        connection_string = connection_dict[key]
                    elif key =="container_name":
                        container_name = connection_dict[key]
                    elif key =="csv_folder":
                        csv_folder = connection_dict[key]
                    


                blob_service_client = BlobServiceClient.from_connection_string(connection_string)





                container_client = blob_service_client.get_container_client(container_name)
                blobs = container_client.list_blobs(name_starts_with=csv_folder)

                # def get_primary_keys_snowflake(connection, table_name):
                #     cursor = connection.cursor()
                #     query = f"""
                #         SHOW PRIMARY KEYS IN TABLE {table_name}
                #     """
                #     cursor.execute(query)
                #     primary_keys = [row[4] for row in cursor.fetchall()]
                    
                #     return primary_keys

                # # Read CSV file names from an Excel file stored in Blob Storage
                csv_files_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx', sheet_name='File_Names')
                print(csv_files_df)
                csv_files = csv_files_df['File_Names'].tolist()


                table_names_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SnowflakeConnection_FileName_TableName.xlsx',sheet_name='Table_Names')
                print(table_names_df)
                table_names = table_names_df['Table_Names'].tolist()

                # Initialize variables for counting unmatched tables
                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0


                for csv_file_input, table_name2 in zip(csv_files, table_names):
                    table_name1 = csv_file_input + '.csv'
                    
                    # # Download CSV file from Azure Blob Storage
                    csv_blob_name = os.path.join(csv_folder, table_name1)
                    blob_client = container_client.get_blob_client(csv_blob_name)
                    # blob_data = blob_client.download_blob()
                    # csv_data = blob_data.readall()
                    # df_csv = pd.read_csv(io.BytesIO(csv_data),dtype=str)

                    try:
                        blob_data = blob_client.download_blob()
                        csv_data = blob_data.readall()
                        df_csv = pd.read_csv(io.BytesIO(csv_data), dtype=str, encoding='utf-8')
                    except Exception as e:
                        print(f"Error: {e}")

                    # print(df_csv)
                    # print("))))))))))))))))))))))))))))))))))))))))))))))))))))))")


                    #   # Check if the blob exists
                    # if not blob_client.exists():
                    #     print(f"The specified blob does not exist: {csv_blob_name}")
                    #     continue  # Skip this iteration and proceed to the next

                    # blob_data = blob_client.download_blob()
                    # csv_data = blob_data.readall()

                    # Create a Pandas DataFrame from the downloaded CSV data
                    


                    query = f"SELECT * FROM {table_name2}"
                    df_snowflake = pd.read_sql(query, conn)

                    # primary_keys_table_snowflake = get_primary_keys_snowflake(conn,table_name2)

                    count_tables_source = len(csv_files)
                    count_tables_target= len(table_names)



                    def get_column_data_types():
                        column_data_types = {}
                        query = f"SHOW COLUMNS IN TABLE {table_name2}"
                        cursor = conn.cursor()
                        cursor.execute(query)
                    # Loop through the result and extract column names and data types
                        for row in cursor:
                            # print(row)
                            column_name = row[2]  # Column name is in the second position
                            data_type = row[3]    # Data type is in the third position
                        
                            dict_data = json.loads(data_type)
                            for data in dict_data:
                                data=dict_data['type']
                            column_data_types[column_name] = data
                            
                        return column_data_types

                    column_data_types_dict = get_column_data_types()

                    df_csv.columns = df_csv.columns.str.upper()
                    columns_selected =[]

                    for column_name in df_csv.columns:
                        if column_name in column_data_types_dict:
                            columns_selected.append(column_name)

                    df_snowflake = df_snowflake[df_csv.columns]
                    df_snowflake = df_snowflake[columns_selected]
                    df_csv = df_csv[columns_selected]



                    for key in column_data_types_dict:
                        if key in columns_selected:
                            if (column_data_types_dict[key]== 'TIMESTAMP_NTZ'):
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d %H:%M')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d %H:%M')
                                

                            elif (column_data_types_dict[key]=="DATE"):
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d')
                                df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                                df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d')

                                
                            elif (column_data_types_dict[key]== 'BOOLEAN'):
                                df_snowflake[key] = df_snowflake[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                                df_csv[key] = df_csv[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                            elif (column_data_types_dict[key]== 'TEXT'):

                                if df_csv[key].dtype == 'float64' or df_csv[key].dtype == 'int64' :
                                    df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)

                    #null-values and str type conversion
                    # df_csv = df_csv.fillna(value='Null')
                    # df_snowflake = df_snowflake.fillna(value='Null')
                    df_csv = df_csv.replace(['nan', 'None', 'Null', 'NaN','NUL','NULL', ''], '')
                    
                    df_snowflake = df_snowflake.replace(['nan', 'None', 'Null','NUL' ,'NaN','NULL', ''], '')
                    df_snowflake = df_snowflake.fillna(value='')
                    df_csv = df_csv.fillna(value='')

                    df_snowflake = df_snowflake[columns_selected]
                    df_csv = df_csv[columns_selected]


                    df_csv = df_csv.astype(str)
                    df_snowflake = df_snowflake.astype(str)

                    comparison = df_csv.equals(df_snowflake)

                    unmatched_rows_snowflake = df_snowflake[~df_snowflake.isin(df_csv)].dropna(how='all').fillna("-")
                    unmatched_rows_snowflake.index = unmatched_rows_snowflake.index + 1  
                    print(unmatched_rows_snowflake)

                    unmatched_rows_csv = df_csv[~df_csv.isin(df_snowflake)].dropna(how='all').fillna("-")
                    unmatched_rows_csv.index = unmatched_rows_csv.index +1
                    print(unmatched_rows_csv)

                    total_rows_csv = len(df_csv)
                    total_rows_snowflake = len(df_snowflake)

                    count_unmatched_csv = 0
                    count_unmatched_snowflake = 0

                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        count_unmatched_csv =len(unmatched_rows_csv.index)
                        count_unmatched_snowflake =len(unmatched_rows_snowflake.index)

                    # Set background color for unmatched rows to red and remaining cells to green
                    unmatched_rows_csv_html = unmatched_rows_csv.to_html(na_rep='-', escape=False)
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_csv.iterrows():
                        for col, value in row.items():
                            if ( value != '-'):
                                unmatched_rows_csv_html = unmatched_rows_csv_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

                    

                    unmatched_rows_snowflake_html =  unmatched_rows_snowflake.to_html(na_rep='-', escape=False)
                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_snowflake.iterrows():
                        for col, value in row.items():
                        
                            if ( value != '-'):
                                unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)
                    
                    


                    html_reports.append({
                        'table_name1': table_name1,
                        'table_name2': table_name2,
                        'comparison_result': comparison,
                        'unmatched_rows_source_html': unmatched_rows_csv_html,
                        'unmatched_rows_target_html': unmatched_rows_snowflake_html,
                        'total_rows_source': total_rows_csv,
                        'total_rows_target': total_rows_snowflake,
                        'count_unmatched_source': count_unmatched_csv,
                        'count_unmatched_target': count_unmatched_snowflake,
                        'compare_url': url_for('compare', table1=table_name1, table2=table_name2)
      


                    })

                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                        }
              
                return render_template('Multiple.html', **context)
                    
        # -------------------------multitable synapse----------------------------------
        elif (database == "synapse"):
            print("++++-------------------------------------------------++++++++++++++")
            data_1 = pd.read_excel("C:\TNDV\Multiple_Tables\SynapseConnection_FileName_TableName.xlsx",sheet_name='Synapse_Connection')

            connection_dict_2 = {}
            for index, row in data_1.iterrows():
                connection_dict_2[row['Connection_Fields'].strip()] = row['Connection_Details']

            for key in connection_dict_2:
                if key == "server":
                    server = connection_dict_2[key]
                elif key =="database":
                    database = connection_dict_2[key]
                elif key =="username":
                    username = connection_dict_2[key]
                
            driver= '{ODBC Driver 17 for SQL Server}'

            connection = (
                    f'DRIVER={driver};'
                    f'SERVER={server};'
                    f'DATABASE={database};'
                    f'Trusted_Connection=no;'
                    f'Authentication=ActiveDirectoryInteractive;'
                    f'MFA=Required;'
                    f'UID={username};'
                )

            #connection_synapse = pyodbc.connect(connection)
            
            if(file_input=="azure_blob_csv"):
                print("-------------------------------------------------------------")
                data_2 = pd.read_excel("C:\TNDV\Multiple_Tables\SynapseConnection_FileName_TableName.xlsx",sheet_name='Blob_Connection')

                connection_dict = {}
                for index, row in data_2.iterrows():
                    connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
                print("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
                # for key in connection_dict:
                #     if key == "connection_string":
                #         connection_string = connection_dict[key]
                #     elif key =="container_name":
                #         container_name = connection_dict[key]
                #     elif key =="csv_folder":
                #         csv_folder = connection_dict[key]
                for key, value in connection_dict.items():
                    if key == "connection_string":
                        connection_string = value
                    elif key == "container_name":
                        container_name = value
                    elif key == "csv_folder":
                        csv_folder = value
                print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")   
                    
                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                container_client = blob_service_client.get_container_client(container_name)
                blobs = container_client.list_blobs(name_starts_with=csv_folder)


                count_tables_source=0
                count_tables_target=0
                # Read CSV file names from an Excel file stored in Blob Storage
                csv_files_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SynapseConnection_FileName_TableName.xlsx', sheet_name='File_Names')
                csv_files = csv_files_df['File_Names'].tolist()
                no_of_rows_files=csv_files_df['No_of_rows'].tolist()
                
                count_tables_source=len(csv_files)
                table_names_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SynapseConnection_FileName_TableName.xlsx',sheet_name='Table_Names')
                table_names = table_names_df['Table_Names'].tolist()
               
                query_file=table_names_df['Query'].tolist()
                count_tables_target=len(table_names)
                
                min_length = min(len(csv_files), len(table_names), len(query_file), len(no_of_rows_files))
                
                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0
                print("hi")
                print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                print("\n\n")
                html_reports=[]
                for index in range(min_length):
                    print(csv_files[index],query_file[index],table_names[index],no_of_rows_files[index])
                    connection_synapse = pyodbc.connect(connection)
                    table_name1 = csv_files[index]
                    csv_blob_name = os.path.join(csv_folder, table_name1)
                    csv_blob_name = csv_blob_name.replace('\\', '/')
                    print("{{{{{{{{{{{{{{{{{{{{{{{{{{{}}}}}}}}}}}}}}}}}}}}}}}}}}}")
                    print(csv_blob_name)
                    print("||||||||||||||||||||||||||||||||||||||||")
                    blob_client = container_client.get_blob_client(csv_blob_name)
                
                    blob_data = blob_client.download_blob()
                    flag=0
                    compressed_data = blob_data.readall()
                    with gzip.open(io.BytesIO(compressed_data), 'rb') as f:
                        df_csv = pd.read_csv(io.StringIO(f.read().decode()), dtype=str)
                        df_csv['ROWINDEX'] = pd.to_numeric(df_csv['ROWINDEX'])
                        df_csv.sort_values(by='ROWINDEX', inplace=True, ignore_index=True, kind='mergesort')
                        if pd.notnull(no_of_rows_files[index]):
                            df_csv = df_csv.head(no_of_rows_files[index])
                        
                            
                    df_csv.columns = df_csv.columns.str.upper()
                    for i in df_csv.columns:
                        if i == "PIPELINERUNTIMEID":
                            df_csv.rename(columns={i: "PIPELINE_RUNTIME_ID"}, inplace=True)
                            
                    # query = f"SELECT  top 100 * FROM {table_name2} order by RowIndex desc"
                    query=query_file[index]
                    query = query.replace(u'\xa0', ' ')
                    query=query.format(table_name=table_names[index])
                    table_name2=table_names[index]
                    df_sql = pd.read_sql(query, connection_synapse)
                    df_sql.columns = df_sql.columns.str.upper()
                    columns_selected =[]
                    
                    for column_name in df_csv.columns:
                        if column_name in df_sql.columns:
                            columns_selected.append(column_name)
                            
                    df_csv =df_csv[columns_selected]
                    df_sql = df_sql[columns_selected]
                    
                    try:
                        cursor = connection_synapse.cursor()
                        parts = table_name2.split('.')
                        if len(parts) == 2:
                            schema, table_name2 = parts
                        else:
                            schema = 'dbo'
                        cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name2}'")

                        column_data_types = {row.COLUMN_NAME.upper(): row.DATA_TYPE for row in cursor.fetchall()}
                    except pyodbc.Error as ex:
                        print("An error occurred:", ex)
                    df_sql = df_sql[df_csv.columns]
                    columns_selected=[]
                    columns_selected = df_csv.columns 
                    for key in column_data_types:
                        if key in columns_selected: 
                            if column_data_types[key] == 'datetime2':
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d %H:%M')
                                df_sql[key] = pd.to_datetime(df_sql[key])
                                df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d %H:%M') 
                            elif column_data_types[key]=="date" :
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d')
                                df_sql[key] = pd.to_datetime(df_sql[key])
                                df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d')
                            elif column_data_types[key]== 'bit':
                                df_sql[key] = df_sql[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                                df_csv[key] = df_csv[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                            elif column_data_types[key] in ['nvarchar', 'int', 'int64', 'bigint','decimal','float']:
                                df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                                df_sql[key] = df_sql[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                    df_csv = df_csv.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
                    df_sql = df_sql.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
                    df_sql = df_sql.fillna(value='Null')
                    df_csv = df_csv.fillna(value='Null')
                    df_sql = df_sql[columns_selected]
                    df_csv = df_csv[columns_selected]
                    df_csv = df_csv.astype(str)
                    df_sql = df_sql.astype(str)
                    unmatched_rows_sql=pd.DataFrame(columns=columns_selected)
                    unmatched_rows_csv=pd.DataFrame(columns=columns_selected)
                    missing_row=pd.DataFrame(columns=columns_selected)
                    new_row = pd.Series(['_' for _ in range(len(columns_selected))], index=columns_selected)
                    missing_row.loc[0]=new_row
                    count=0
                    
                    for i in range(len(df_sql)):
                        value_to_match = df_sql['ROWINDEX'].iloc[i]
                        matching_indices_df1 = df_sql.index[df_sql['ROWINDEX'] == value_to_match]
                        matching_indices_df2 = df_csv.index[df_csv['ROWINDEX'] == value_to_match]
                        index_list_sql = matching_indices_df1.tolist()
                        index_list_csv = matching_indices_df2.tolist()
                        index_sql=index_list_sql[0]
                        if index_list_csv:
                            index_csv=index_list_csv[0]
                            # print(index_sql,index_csv)
                            row_df1 = df_csv.loc[index_csv]
                            row_df1=pd.DataFrame([row_df1])
                            row_df2 = df_sql.loc[index_sql]
                            row_df2=pd.DataFrame([row_df2])
                            # comparison = row_df1.equals(row_df2)
                            row_str_1 = row_df1.astype(str).values.tolist()
                            row_str_2 = row_df2.astype(str).values.tolist()
                            comparison_str = row_str_1 == row_str_2
                            if comparison_str:
                                count+=1
                            
                            
                            if not comparison_str:
                                flag=1
                                unmatched_row_sql = row_df2[~row_df2.isin(row_df1)].dropna(how='all').fillna("-")
                                unmatched_rows_sql=pd.concat([unmatched_rows_sql,unmatched_row_sql], ignore_index=True)
                                unmatched_row_csv = row_df1[~row_df1.isin(row_df2)].dropna(how='all').fillna("-")
                                unmatched_rows_csv=pd.concat([unmatched_rows_csv,unmatched_row_csv], ignore_index=True)
                        #     print("=====================================================") 
                        #     print(unmatched_rows_csv) 
                        #     print(unmatched_rows_sql)
                        #     print("===========================================")     
                        else:
                            flag=1
                            row_df2 = df_sql.loc[i]
                            row_df2=pd.DataFrame([row_df2])
                            # print(row_df2)
                            unmatched_rows_sql=pd.concat([unmatched_rows_sql, row_df2], ignore_index=True)
                            unmatched_rows_csv=pd.concat([unmatched_rows_csv,missing_row], ignore_index=True)
                    comparison=True 
                    
                    for i in range(0,len(df_csv)): 
                        value_to_match = df_csv['ROWINDEX'].iloc[i] 
                        matching_indices_df1 = df_sql.index[df_sql['ROWINDEX'] == value_to_match]
                        matching_indices_df2 = df_csv.index[df_csv['ROWINDEX'] == value_to_match]
                        index_list_sql = matching_indices_df1.tolist()
                        index_list_csv = matching_indices_df2.tolist()
                        if not index_list_sql: 
                            flag=1
                            row_df1 = df_csv.loc[i]
                            row_df1=pd.DataFrame([row_df1])
                            unmatched_rows_csv=pd.concat([unmatched_rows_csv,row_df1], ignore_index=True)
                            unmatched_rows_sql=pd.concat([unmatched_rows_sql,missing_row], ignore_index=True)
                    if flag==1:
                        count_unmatched_tables_source +=1
                        count_unmatched_tables_target +=1
                        comparison=False
                                
                   
                    print(unmatched_rows_csv) 
                    print(unmatched_rows_sql)
                    unmatched_rows_sql_html = unmatched_rows_sql.to_html(na_rep='-', escape=False)
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<tr>', '<tr style="text-align: center">')  
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<td>_</td>', '<td style="background-color: red"> </td>') 
                    
                    for index, row in unmatched_rows_sql.iterrows():
                       for col, value in row.items():
                            if ( value != '-'):
                                unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)       
                    unmatched_rows_csv_html = unmatched_rows_csv.to_html(na_rep='-', escape=False)
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<tr>', '<tr style="text-align: center">')  
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<td>_</td>', '<td style="background-color: red"> </td>') 
                    
                    for index, row in unmatched_rows_csv.iterrows():
                        for col, value in row.items():
                            if ( value != '-'):
                                unmatched_rows_csv_html = unmatched_rows_csv_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)       
                    
                    if count==0:
                        total_comparison=True
                    else:
                        total_comparison=False
                        
                    
                    connection_synapse.close()
                    
                    html_reports.append({
                            'table_name1': table_name1,
                            'table_name2': table_name2,
                            'comparison_result': comparison,
                            'unmatched_rows_source_html': unmatched_rows_csv_html,
                            'unmatched_rows_target_html': unmatched_rows_sql_html,
                            'total_rows_source': len(df_csv),
                            'total_rows_target': len(df_sql),
                            'count_unmatched_source': len(unmatched_rows_csv),
                            'count_unmatched_target': len(unmatched_rows_sql),
                            'compare_url': url_for('compare', table1=table_name1, table2=table_name2),
                            'total_comparision':total_comparison
                            
                            
                            })
                  
                    
                print(len(html_reports))  
                print(count_tables_source) 
                print(count_tables_target)
                print( count_unmatched_tables_source)   
                print(count_unmatched_tables_target)
                    
                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                            }
                    
            
                return render_template('Multiple.html', **context)
            
            
        elif(database=="azure_blob_csv"):
            if(file_input=="azure_blob_csv"):
                def connection(data):
                    connection_dict = {}
                    for index, row in data.iterrows():
                        connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
                        print(connection_dict)
                        for key in connection_dict:
                            print(key)
                            if key == "connection_string":
                                connection_string = connection_dict[key]
                            elif key =="container_name":
                                container_name = connection_dict[key]
                            elif key =="csv_folder":
                                csv_folder = connection_dict[key]                   
                    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                    container_client = blob_service_client.get_container_client(container_name)
                    blobs = container_client.list_blobs(name_starts_with=csv_folder)
                    return blob_service_client, container_client, blobs,csv_folder
                data1=pd.read_excel(r'C:\TNDV\Multiple_Tables\BlobToBlob_Connection.xlsx', sheet_name='Blob_Connection_1')
                data2=pd.read_excel(r'C:\TNDV\Multiple_Tables\BlobToBlob_Connection.xlsx', sheet_name='Blob_Connection_2')
                
                blob_service_client1,container_client1,blobs1,csv_folder1=connection(data1)
                blob_service_client2,container_client2,blobs2,csv_folder2=connection(data2)


                


                csv_files_df1 = pd.read_excel(r'C:\TNDV\Multiple_Tables\BlobToBlob_Connection.xlsx', sheet_name='File_names_1')
                csv_files1 = csv_files_df1['File_Names'].tolist()
                print(csv_files1)
                no_of_rows_files1=csv_files_df1['No_of_rows'].tolist()
                print(no_of_rows_files1)
                columns1=csv_files_df1['columns'].tolist()


                csv_files_df2 = pd.read_excel(r'C:\TNDV\Multiple_Tables\BlobToBlob_Connection.xlsx', sheet_name='File_names_2')
                csv_files2 = csv_files_df2['File_Names'].tolist()
                no_of_rows_files2=csv_files_df1['No_of_rows'].tolist()
                columns2=csv_files_df2['columns'].tolist()
                
                
                min_len=min(len(csv_files1),len(csv_files2))
                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0
                count_tables_source=len(csv_files1)
                count_tables_target=len(csv_files2)
                
                def compress(csv_folder,csv_file,container_client,no_of_rows,columns):
                    df_csv=pd.DataFrame()
                    if  csv_file.endswith('.gz'):
                        csv_blob_name = os.path.join(csv_folder, csv_file)
                        csv_blob_name = csv_blob_name.replace('\\', '/')
                    
                        blob_client = container_client.get_blob_client(csv_blob_name)
                        blob_data = blob_client.download_blob()
                        compressed_data = blob_data.readall()
                        with gzip.open(io.BytesIO(compressed_data), 'rb') as f:
                            df_csv = pd.read_csv(io.StringIO(f.read().decode()), dtype=str)
                    elif csv_file.endswith('.csv'):
                        print("entered Csv")
                        csv_blob_name = os.path.join(csv_folder, csv_file)
                        # csv_blob_name = csv_blob_name.replace('\\', '/')
                        blob_client = container_client.get_blob_client(csv_blob_name)
                        try:
                            blob_data = blob_client.download_blob()
                            csv_data = blob_data.readall()
                            df_csv = pd.read_csv(io.BytesIO(csv_data), dtype=str, encoding='utf-8')
                            print(df_csv)
                        except Exception as e:
                            print(f"Error: {e}")
                        # df_csv = pd.read_csv(io.BytesIO(compressed_data), dtype=str)
                    # df_csv.columns = df_csv.columns.str.upper()
                        # df_csv['ROWINDEX'] = pd.to_numeric(df_csv['ROWINDEX'])
                        # df_csv.sort_values(by='ROWINDEX', inplace=True, ignore_index=True, kind='mergesort')
                        
                    if pd.notnull(no_of_rows):
                        df_csv = df_csv.head(no_of_rows)
                    if pd.notnull(columns):
                        selected_columns = columns.strip('[]').replace(' ', '').split(',')
                        df_csv=df_csv[selected_columns]
                    
                            
                        # df_csv = df_csv.sort_values(by=columns_for_sorting)
                    return df_csv,csv_file
                    

                count_tables_source=0
                count_tables_target=0
                count_tables_source=len(csv_files1)
                count_tables_target=len(csv_files2)
                print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                for i in range(min_len):
                    total_comparision=True
                    df_csv2,table_name2=compress(csv_folder2, csv_files2[i],container_client2,no_of_rows_files2[i],columns2[i])
                    print("===================================================================")
                    
                    df_csv1,table_name1=compress(csv_folder1, csv_files1[i],container_client1,no_of_rows_files1[i],columns1[i])
                            
                    print("9999999999999999999999999999999999999999999999999999999999999999999")  
                        
                
                    columns_selected=[]       
                    for column_name in df_csv1.columns:
                        if column_name in df_csv2.columns:
                            columns_selected.append(column_name)
                                        
                    df_csv1 =df_csv1[columns_selected]
                    df_csv2 =df_csv2[columns_selected]
                    df_csv1= df_csv1.sort_values(by=columns_selected)
                    df_csv2 = df_csv2.sort_values(by=columns_selected)
                    print("00000000000000000000000000000000000000000000000000000")
                    print(df_csv1)
                    print(df_csv2)
                    
                    df_csv1 = df_csv1.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
                    df_csv2 = df_csv2.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
                    df_csv1 = df_csv1.fillna(value='Null')
                    df_csv2 = df_csv2.fillna(value='Null')
                    # df_csv1 = df_csv1[columns_selected]
                    # df_csv2 = df_csv2[columns_selected]
                    df_csv1 = df_csv1.astype(str)
                    df_csv2 = df_csv2.astype(str)
                    

                    comparison = df_csv1.equals(df_csv2)
                    
                    unmatched_rows_csv_1 = df_csv1[~df_csv1.isin(df_csv2)].dropna(how='all').fillna("-")
                    unmatched_rows_csv_1.index = unmatched_rows_csv_1.index + 1  
                    print(unmatched_rows_csv_1)

                    unmatched_rows_csv_2 = df_csv2[~df_csv2.isin(df_csv1)].dropna(how='all').fillna("-")
                    unmatched_rows_csv_2.index = unmatched_rows_csv_2.index +1
                    print(unmatched_rows_csv_2)

                
                    total_rows_csv_1= len(df_csv1)
                    print("LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL")
                    print(total_rows_csv_1)
                    total_rows_csv_2 = len(df_csv2)
                    
                    print(total_rows_csv_2)
                    print("LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL")
                    count_unmatched_df1 = 0
                    count_unmatched_df2 = 0
                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        len_unmatched_csv1 =len(unmatched_rows_csv_1.index)
                        len_unmatched_csv2=len(unmatched_rows_csv_2.index)
                    
                        
                    if len_unmatched_csv1>1000:
                        unmatched_rows_csv_1=unmatched_rows_csv_1.iloc[0:1000]
                    if len_unmatched_csv2>1000:
                        unmatched_rows_csv_2=unmatched_rows_csv_2.iloc[0:1000] 
                    if len(df_csv1)<=len_unmatched_csv1 or len(df_csv2)<=len_unmatched_csv2:
                        total_comparision=True
                        len_unmatched_csv1=len(df_csv1)
                        len_unmatched_csv2=len(df_csv2)
                    else:
                        total_comparision=False
                        
                        
                    unmatched_rows_csv1_html = unmatched_rows_csv_1.to_html(na_rep='-', escape=False)
                    unmatched_rows_csv1_html = unmatched_rows_csv1_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_csv1_html = unmatched_rows_csv1_html.replace('<tr>', '<tr style="text-align: center">')
                    for index, row in unmatched_rows_csv_1.iterrows():
                        for col, value in row.items():
                            if ( value != '-'):
                                unmatched_rows_csv1_html = unmatched_rows_csv1_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)
                    unmatched_rows_csv2_html = unmatched_rows_csv_2.to_html(na_rep='-', escape=False)
                    unmatched_rows_csv2_html = unmatched_rows_csv2_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_csv2_html = unmatched_rows_csv2_html.replace('<tr>', '<tr style="text-align: center">')
                    for index, row in unmatched_rows_csv_2.iterrows():
                        for col, value in row.items():
                            if ( value != '-'):
                                unmatched_rows_csv2_html = unmatched_rows_csv2_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)
                    file_table="file"
                    html_reports.append({
                                'table_name1': table_name1,
                                'table_name2': table_name2,
                                'comparison_result': comparison,
                                'unmatched_rows_source_html': unmatched_rows_csv1_html,
                                'unmatched_rows_target_html': unmatched_rows_csv2_html,
                                'total_rows_source': len(df_csv1),
                                'total_rows_target': len(df_csv2),
                                'count_unmatched_source':len_unmatched_csv1 ,
                                'count_unmatched_target': len_unmatched_csv2 ,
                                'compare_url': url_for('compare', table1=table_name1, table2=table_name2),
                                'total_comparision':total_comparision,
                                'file_table':file_table
                                
                                
                                })
                    print("done")
                context = {'reports': html_reports,
                                'count_tables_source': count_tables_source,
                                'count_tables_target': count_tables_target,
                                'count_unmatched_tables_source': count_unmatched_tables_source,
                                'count_unmatched_tables_target': count_unmatched_tables_target
                                }
                     
                
                return render_template('Multiple.html', **context)
                  
                                        
                            
                
        ###############-----------------MultiTable SQL Server--------------------###############################

        else:

            data = pd.read_excel('C:\TNDV\Multiple_Tables\SQLServerConnection_FileName_TableName.xlsx',sheet_name='Connection')
            print(data)
            connection_dict = {}
            for index, row in data.iterrows():
                connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
            print(connection_dict)
            for key in connection_dict:
                print(key)
                if key == "server":
                    server_sq = connection_dict[key]

                elif key =="database":
                    database_sq = connection_dict[key]
                
                elif key =="username":
                    username_sq = connection_dict[key]
                
            password_sq = password
            print(username_sq)
            print(password_sq) 

            conn= pyodbc.connect(
                    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server_sq + ';DATABASE=' + database_sq + ';UID=' + username_sq + ';PWD=' + password_sq
                )
            

            
            def get_primary_keys_sql(connection, table_name):
                        table_name = table_name.split('.')[1]
                        print(table_name,"+++++++++++++++++++++++++++++++++++++++++++++++++")
                        query = """
                            SELECT COLUMN_NAME
                            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
                            AND TABLE_NAME = ?
                        """
                        primary_keys_df = pd.read_sql(query, connection, params=[table_name])
                
                        return primary_keys_df['COLUMN_NAME'].tolist()
            

           
                

            ###############-----------------SQL Server and  json--------------------###############################
            if (file_input== "json"):
                

                json_files_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SQLServerConnection_FileName_TableName.xlsx', sheet_name=file_name)

                json_files = json_files_df['File_Names'].tolist()





                csv_file_path = "C:\\TNDV\\Multiple_Tables\\Folder_Path_MultipleTables.csv"
                df_csv = pd.read_csv(csv_file_path)
            
                # Loop through each row in the CSV
                for index, row in df_csv.iterrows():
                    json_folder = row['json_path'].strip('"')  # Assuming 'json_path' is the column containing the JSON path
                 

                table_names_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SQLServerConnection_FileName_TableName.xlsx', sheet_name=table)
                table_names = table_names_df['Table_Names'].tolist()
                print(table_names,"****************************************************************")



                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0

                for json_file_input, table_name2 in zip(json_files, table_names):
                    table_name1 = json_file_input + '.json'
                    json_file = os.path.join(json_folder,table_name1 )

                    data_dict = pd.read_json(json_file)
                    df_json = pd.DataFrame(data_dict)
                   

                    query = f"SELECT * FROM {table_name2}"

                    df_sql = pd.read_sql(query, conn)

                    primary_keys_table_sql = get_primary_keys_sql(conn, table_name2)

                   

                    

                    count_tables_source = len(json_files)
                    count_tables_target= len(table_names)

                    
                    cursor = conn.cursor()
                    parts = table_name2.split('.')
                    if len(parts) == 2:
                        schema, table_name2 = parts
                    else:
                        schema = 'dbo'  # Default schema if not provided

                    cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name2}'")
                    
                    column_data_types = {row.COLUMN_NAME: row.DATA_TYPE for row in cursor.fetchall()}

                    print(column_data_types)

                    df_sql = df_sql[df_json.columns]
                  

                    print(df_json.columns)
                    columns_selected=[]
                 
                    columns_selected = df_json.columns

                    for key in column_data_types:
                        for key in columns_selected:
                            if column_data_types[key] == 'datetime2':
                                
                            
                                df_json[key] = pd.to_datetime(df_json[key])
                                df_json[key] = df_json[key].dt.strftime('%Y-%m-%d %H:%M')
                                df_sql[key] = pd.to_datetime(df_sql[key])
                                df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d %H:%M')

                            elif column_data_types[key]=="date" :
                                df_json[key] = pd.to_datetime(df_json[key])
                                df_json[key] = df_json[key].dt.strftime('%Y-%m-%d')

                                df_sql[key] = pd.to_datetime(df_sql[key])
                                df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d')
                            elif column_data_types[key]== 'bit':
                                print("\n\n\n")
                              
                                df_sql[key] = df_sql[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                                df_json[key] = df_json[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                            elif column_data_types[key] in ['nvarchar', 'int', 'int64', 'bigint']:
                            

                            
                                df_json[key] = df_json[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                                df_sql[key] = df_sql[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                        

                
                    df_json.replace(['', np.nan], 'NULL', inplace=True)
                    # Replace empty cells with None in the SQL Server dataframe
                    df_sql.replace(['', np.nan], 'NULL', inplace=True)
                    df_sql.replace([''], 'NULL')

                    df_sql_selected = df_sql[columns_selected]
                  
                    df_json_selected = df_json[columns_selected]
                    df_json_selected =df_json_selected.astype(str)
                    df_sql_selected = df_sql_selected.astype(str)


                 
                    comparison = df_json_selected.equals(df_sql_selected)

                            
                    # Find unmatched rows
                    unmatched_rows_sql = df_sql[~df_sql_selected.isin(df_json_selected)].dropna(how='all')
                    for col in primary_keys_table_sql:
                        unmatched_rows_sql[col] = df_sql[col]
                    unmatched_rows_sql.index = unmatched_rows_sql.index + 1  
                    unmatched_rows_sql = unmatched_rows_sql[~unmatched_rows_sql.isin(primary_keys_table_sql)].dropna(how='all')
                

                
                    primary_keys_file  =  [col for col in primary_keys_table_sql if col in df_json.columns]
                    if primary_keys_file:

                        unmatched_rows_json = df_json[~df_json_selected.isin(df_sql_selected)].dropna(how='all')
                        for col in primary_keys_file:
                            unmatched_rows_json[col] = df_json[col] 
                        unmatched_rows_json.index = unmatched_rows_json.index + 1  
                        unmatched_rows_json = unmatched_rows_json[~unmatched_rows_json.isin(primary_keys_file)].dropna(how='all')
                

                    total_rows_json = len(df_json)
                    total_rows_sql = len(df_sql)

                    count_unmatched_json = 0
                    count_unmatched_sql = 0

                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        count_unmatched_json =len(unmatched_rows_json.index)
                        count_unmatched_sql =len(unmatched_rows_sql.index)

                    # Set background color for unmatched rows to red and remaining cells to green
                    unmatched_rows_json_html = unmatched_rows_json.to_html(na_rep='-', escape=False)
                    unmatched_rows_json_html = unmatched_rows_json_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_json_html = unmatched_rows_json_html.replace('<tr>', '<tr style="text-align: center">')


                    # Iterate over the DataFrame to set red background for non-matching cells
                    for index, row in unmatched_rows_json.iterrows():
                        for col, value in row.items():
                        
                        
                                if col in primary_keys_file:
                                    unmatched_rows_json_html = unmatched_rows_json_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                                elif ( value != '-' and col not in primary_keys_file):
                                    unmatched_rows_json_html = unmatched_rows_json_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)




                    unmatched_rows_sql_html = unmatched_rows_sql.to_html(na_rep='-', escape=False)
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_sql.iterrows():
                                for col, value in row.items():
                                    
                                        if col in primary_keys_table_sql:
                                            unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                                        elif ( value != '-' and col not in primary_keys_table_sql):
                                            value_to_replace = f'<td>{str(value).strip()}</td>'
                                            replacement_value = f'<td style="background-color: red">{str(value).strip()}</td>'
                                            unmatched_rows_sql_html = unmatched_rows_sql_html.replace(value_to_replace, replacement_value,1)





                    html_reports.append({
                        'table_name1': table_name1,
                        'table_name2': table_name2,
                        'comparison_result': comparison,
                        'unmatched_rows_source_html': unmatched_rows_json_html,
                        'unmatched_rows_target_html': unmatched_rows_sql_html,
                        'total_rows_source': total_rows_json,
                        'total_rows_target': total_rows_sql,
                        'count_unmatched_source': count_unmatched_json,
                        'count_unmatched_target': count_unmatched_sql,
                        'compare_url': url_for('compare', table1=table_name1, table2=table_name2)



                    })

                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                        }
                return render_template('Multiple.html', **context)
            



            
            ###############-----------------SQL Server VS Flatfile --------------------###############################



            elif(file_input== "FlatFile"):

                csv_files_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SQLServerConnection_FileName_TableName.xlsx', sheet_name=file_name)
                print(csv_files_df)
                csv_files = csv_files_df['File_Names'].tolist()
               

                csv_file_path = "C:\\TNDV\\Multiple_Tables\\Folder_Path_MultipleTables.csv"
                df_csv = pd.read_csv(csv_file_path)
                
                # Loop through each row in the CSV
                for index, row in df_csv.iterrows():
                    #json_path1 = row['json_path']  # Assuming 'json_path' is the column containing the JSON path
                    csv_folder = row['flat_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path


           
                table_names_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SQLServerConnection_FileName_TableName.xlsx', sheet_name=table)
                print(table_names_df)
                table_names = table_names_df['Table_Names'].tolist()

                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0

                for csv_file_input, table_name2 in zip(csv_files, table_names):
                    table_name1 = csv_file_input + '.txt'
                    csv_file = os.path.join(csv_folder,table_name1)


                    #determining the delimiter of the flat table_name1
                    def determine_delimiter(file_path, num_lines=5):
                        comma_count = 0
                        tab_count = 0
                        pipe_count = 0


                        with open(file_path, 'r') as file:
                            for _ in range(num_lines):
                                line = file.readline()
                                comma_count += line.count(',')
                                tab_count += line.count('\t')
                                pipe_count +=line.count('|')

                        if comma_count > tab_count and comma_count > pipe_count:
                            return ','
                        elif tab_count > comma_count and tab_count > pipe_count:
                            return '\t'
                        elif pipe_count > comma_count and pipe_count > tab_count:
                            return '|'
                        else:
                            return None  # Use the default delimiter
                        
                    separator = determine_delimiter(csv_file)


                    query = f"SELECT * FROM {table_name2}"
                    df_sql = pd.read_sql(query, conn)

                    primary_keys_table_sql = get_primary_keys_sql(conn, table_name2)


                    count_tables_source = len(csv_files)
                    count_tables_target= len(table_names)

                    cursor = conn.cursor()
                    parts = table_name2.split('.')
                    if len(parts) == 2:
                        schema, table_name2 = parts
                    else:
                        schema = 'dbo'  # Default schema if not provided

                    print(schema,table_name2,"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

                    # cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'")
                    
                    # print(f"Executing query: SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name2}'")
                    cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name2}'")
                    # print("Query executed ********************************************************************************")
                    column_data_types = {row.COLUMN_NAME: row.DATA_TYPE for row in cursor.fetchall()}
                    # print(column_data_types,"++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")


                    if separator:
                        df_csv = pd.read_csv(csv_file, delimiter=separator)
                    else:
                        raise ValueError("Could not determine the delimiter of the flat file")
                    # print(df_sql,"*********************************")
                    # print(df_csv,"*******************************")

                   
                    df_sql = df_sql[df_csv.columns]
                    columns_selected=[]
            
                    columns_selected = df_csv.columns
                    

                    for key in column_data_types:
                        print("Debug: Inside loop with key:", key)
                        if key in columns_selected:
                            print("Debug: Inside loop with column_data_types key:", key, "and columns_selected key:", key)


                            if column_data_types[key] == 'datetime2':
                                # print(key)
                            

                                # print("\n\n\n")
                                print("(((((((((((((((((((((((((())))))))))))))))))))))))))")
                        
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d %H:%M')
                                df_sql[key] = pd.to_datetime(df_sql[key])
                                df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d %H:%M')
                            

                            elif column_data_types[key]=="date" :
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d')

                                df_sql[key] = pd.to_datetime(df_sql[key])
                                df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d')
                            

                            elif column_data_types[key]== 'bit':
                                # print("+++++++++++++++++++++++++++++++++++++++++++++")
                                
                                df_sql[key] = df_sql[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                                df_csv[key] = df_csv[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                            elif column_data_types[key] in ['nvarchar', 'int', 'int64', 'bigint']:
                            

                                df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                                df_sql[key] = df_sql[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                                    


                    df_csv = df_csv.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
                    df_sql = df_sql.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
                    df_sql = df_sql.fillna(value='Null')
                    df_csv = df_csv.fillna(value='Null')

                    df_sql_selected = df_sql[columns_selected]
                        #df_csv_selected = df_csv[csv_columns]
                    df_csv_selected = df_csv[columns_selected]


                    df_csv_selected = df_csv_selected.astype(str)
                    df_sql_selected = df_sql_selected.astype(str)
                    comparison = df_csv_selected.equals(df_sql_selected)

                    unmatched_rows_sql = df_sql[~df_sql_selected.isin(df_csv_selected)].dropna(how='all')
                    for col in primary_keys_table_sql:
                        unmatched_rows_sql[col] = df_sql[col]
                    unmatched_rows_sql.index = unmatched_rows_sql.index + 1  
                    unmatched_rows_sql = unmatched_rows_sql[~unmatched_rows_sql.isin(primary_keys_table_sql)].dropna(how='all')
                

                
                    primary_keys_file  =  [col for col in primary_keys_table_sql if col in df_csv.columns]
                    if primary_keys_file:

                        unmatched_rows_csv = df_csv[~df_csv_selected.isin(df_sql_selected)].dropna(how='all')
                        for col in primary_keys_file:
                            unmatched_rows_csv[col] = df_csv[col] 
                        unmatched_rows_csv.index = unmatched_rows_csv.index + 1  
                        unmatched_rows_csv = unmatched_rows_csv[~unmatched_rows_csv.isin(primary_keys_file)].dropna(how='all')


                    total_rows_csv = len(df_csv)
                    total_rows_sql = len(df_sql)

                    count_unmatched_csv = 0
                    count_unmatched_sql = 0

                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        count_unmatched_csv =len(unmatched_rows_csv.index)
                        count_unmatched_sql =len(unmatched_rows_sql.index)


                    # Set background color for unmatched rows to red and remaining cells to green
                    unmatched_rows_csv_html = unmatched_rows_csv.to_html(na_rep='-', escape=False)
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<tr>', '<tr style="text-align: center">')


                    # Iterate over the DataFrame to set red background for non-matching cells
                    for index, row in unmatched_rows_csv.iterrows():
                        for col, value in row.items():
                        
                            
                                if col in primary_keys_file:
                                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                                elif ( value != '-' and col not in primary_keys_file):
                                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)




                    unmatched_rows_sql_html = unmatched_rows_sql.to_html(na_rep='-', escape=False)
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_sql.iterrows():
                                for col, value in row.items():
                                    
                                        if col in primary_keys_table_sql:
                                            unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                                        elif ( value != '-' and col not in primary_keys_table_sql):
                                            unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)



                    html_reports.append({
                        'table_name1': table_name1,
                        'table_name2': table_name2,
                        'comparison_result': comparison,
                        'unmatched_rows_source_html': unmatched_rows_csv_html,
                        'unmatched_rows_target_html': unmatched_rows_sql_html,
                        'total_rows_source': total_rows_csv,
                        'total_rows_target': total_rows_sql,
                        'count_unmatched_source': count_unmatched_csv,
                        'count_unmatched_target': count_unmatched_sql,
                        'compare_url': url_for('compare', table1=table_name1, table2=table_name2)



                    })

                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                        }
                return render_template('Multiple.html', **context)
            
            ###############-----------------SQL Server VS csv --------------------###############################

            elif (file_input == "csv"):

                csv_files_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SQLServerConnection_FileName_TableName.xlsx', sheet_name=file_name)
                print(csv_files_df)
                csv_files = csv_files_df['File_Names'].tolist()
           

                csv_file_path = "C:\\TNDV\\Multiple_Tables\\Folder_Path_MultipleTables.csv"
                df_csv = pd.read_csv(csv_file_path)
                
                # Loop through each row in the CSV
                for index, row in df_csv.iterrows():
                   
                    csv_folder= row['csv_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path

                table_names_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SQLServerConnection_FileName_TableName.xlsx', sheet_name=table)
                print(table_names_df)
                table_names = table_names_df['Table_Names'].tolist()

                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0

                for csv_file_input, table_name2 in zip(csv_files, table_names):


                    table_name1 = csv_file_input + '.csv'
                    csv_file = os.path.join(csv_folder,table_name1 )

                    
                    df_csv = pd.read_csv(csv_file)
                    

                    query = f"SELECT * FROM {table_name2}"
                    df_sql = pd.read_sql(query, conn)

                    primary_keys_table_sql = get_primary_keys_sql(conn, table_name2)
                

                    count_tables_source = len(csv_files)
                    count_tables_target= len(table_names)

                    cursor = conn.cursor()
                    parts = table_name2.split('.')
                    if len(parts) == 2:
                        schema, table_name2 = parts
                    else:
                        schema = 'dbo'  # Default schema if not provided

                    cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name2}'")
                    
                    column_data_types = {row.COLUMN_NAME: row.DATA_TYPE for row in cursor.fetchall()}

                    print(column_data_types)
                    df_sql = df_sql[df_csv.columns]
                    columns_selected=[]
            
                    columns_selected = df_csv.columns

                    for key in column_data_types:
                        if key in columns_selected:

                            if column_data_types[key] == 'datetime2':
                                print(key)
                            

                                print("\n\n\n")
                        
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d %H:%M')
                                df_sql[key] = pd.to_datetime(df_sql[key])
                                df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d %H:%M')
                            

                            elif column_data_types[key]=="date" :
                                df_csv[key] = pd.to_datetime(df_csv[key])
                                df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d')

                                df_sql[key] = pd.to_datetime(df_sql[key])
                                df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d')
                            

                            elif column_data_types[key]== 'bit':
                                
                                df_sql[key] = df_sql[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                                df_csv[key] = df_csv[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                            elif column_data_types[key] in ['nvarchar', 'int', 'int64', 'bigint']:
                            

                                df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                                df_sql[key] = df_sql[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                                    

                    df_csv = df_csv.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
                    df_sql = df_sql.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
                    df_sql = df_sql.fillna(value='Null')
                    df_csv = df_csv.fillna(value='Null')
                    df_sql_selected = df_sql[columns_selected]
              
                    df_csv_selected = df_csv[columns_selected]
                
                    df_csv_selected = df_csv_selected.astype(str)
                    df_sql_selected = df_sql_selected.astype(str)
                    comparison = df_csv_selected.equals(df_sql_selected)

                    unmatched_rows_sql = df_sql[~df_sql_selected.isin(df_csv_selected)].dropna(how='all')
                    for col in primary_keys_table_sql:
                        unmatched_rows_sql[col] = df_sql[col]
                    unmatched_rows_sql.index = unmatched_rows_sql.index + 1  
                    unmatched_rows_sql = unmatched_rows_sql[~unmatched_rows_sql.isin(primary_keys_table_sql)].dropna(how='all')
                

                
                    primary_keys_file  =  [col for col in primary_keys_table_sql if col in df_csv.columns]
                    if primary_keys_file:

                        unmatched_rows_csv = df_csv[~df_csv_selected.isin(df_sql_selected)].dropna(how='all')
                        for col in primary_keys_file:
                            unmatched_rows_csv[col] = df_csv[col] 
                        unmatched_rows_csv.index = unmatched_rows_csv.index + 1  
                        unmatched_rows_csv = unmatched_rows_csv[~unmatched_rows_csv.isin(primary_keys_file)].dropna(how='all')

                    total_rows_csv = len(df_csv)
                    total_rows_sql = len(df_sql)
                    count_unmatched_csv = 0
                    count_unmatched_sql = 0

                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        count_unmatched_csv =len(unmatched_rows_csv.index)
                        count_unmatched_sql =len(unmatched_rows_sql.index)


        
                    # Set background color for unmatched rows to red and remaining cells to green
                    unmatched_rows_csv_html = unmatched_rows_csv.to_html(na_rep='-', escape=False)
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace('<tr>', '<tr style="text-align: center">')

                    # Iterate over the DataFrame to set red background for non-matching cells
                    for index, row in unmatched_rows_csv.iterrows():
                        for col, value in row.items():
                        
                            
                                if col in primary_keys_file:
                                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                                elif ( value != '-' and col not in primary_keys_file):
                                    unmatched_rows_csv_html = unmatched_rows_csv_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)




                    unmatched_rows_sql_html = unmatched_rows_sql.to_html(na_rep='-', escape=False)
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_sql.iterrows():
                                for col, value in row.items():
                                
                                        if col in primary_keys_table_sql:
                                            unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                                        elif ( value != '-' and col not in primary_keys_table_sql):
                                            unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)



                    html_reports.append({
                        'table_name1': table_name1,
                        'table_name2': table_name2,
                        'comparison_result': comparison,
                        'unmatched_rows_source_html': unmatched_rows_csv_html,
                        'unmatched_rows_target_html': unmatched_rows_sql_html,
                        'total_rows_source': total_rows_csv,
                        'total_rows_target': total_rows_sql,
                        'count_unmatched_source': count_unmatched_csv,
                        'count_unmatched_target': count_unmatched_sql,
                        'compare_url': url_for('compare', table1=table_name1, table2=table_name2)


                    })

                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                        }
                return render_template('Multiple.html', **context)
            

            ###############-----------------SQL VS xml --------------------###############################

            elif (file_input == "xml"):
                xml_files_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SQLServerConnection_FileName_TableName.xlsx', sheet_name=file_name)
                print(xml_files_df)
                xml_files = xml_files_df['File_Names'].tolist()
                print(xml_files,"***********************************")


                csv_file_path = "C:\\TNDV\\Multiple_Tables\\Folder_Path_MultipleTables.csv"

                df_csv = pd.read_csv(csv_file_path)
                # Loop through each row in the CSV

                for index, row in df_csv.iterrows():

                    xml_folder = row['xml_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path



        
                table_names_df = pd.read_excel(r'C:\TNDV\Multiple_Tables\SQLServerConnection_FileName_TableName.xlsx', sheet_name=table)
                print(table_names_df)
                table_names = table_names_df['Table_Names'].tolist()

                count_unmatched_tables_source = 0
                count_unmatched_tables_target = 0

                for xml_file_input, table_name2 in zip(xml_files, table_names):
                    table_name1 = xml_file_input + '.xml'
                    xml_file = os.path.join(xml_folder,table_name1)


                    #determining the delimiter of the flat table_name1
                    tree = ET.parse(xml_file)
                    root = tree.getroot()
                    xml_data = []
                    for child in root:
                        row = {}
                        for subchild in child:
                    
                            row[subchild.tag] = subchild.text
                        xml_data.append(row)
                    df_xml = pd.DataFrame(xml_data)

                    query = f"SELECT * FROM {table_name2}"
                    df_sql = pd.read_sql(query, conn)

                    primary_keys_table_sql = get_primary_keys_sql(conn, table_name2)


                    count_tables_source = len(xml_files)
                    count_tables_target= len(table_names)


                    cursor = conn.cursor()
                    parts = table_name2.split('.')
                    if len(parts) == 2:
                        schema, table_name2 = parts
                    else:
                        schema = 'dbo'  # Default schema if not provided

                    cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name2}'")
                    
                    column_data_types = {row.COLUMN_NAME: row.DATA_TYPE for row in cursor.fetchall()}

                    print(column_data_types)

                    df_sql = df_sql[df_xml.columns]
                    columns_selected=[]
            
                    columns_selected = df_xml.columns

                    for key in column_data_types:
                        for key in columns_selected:
                            if column_data_types[key] == 'datetime2':
                                print(key)
                
                                df_xml[key] = pd.to_datetime(df_xml[key])
                                df_xml[key] = df_xml[key].dt.strftime('%Y-%m-%d %H:%M')
                                df_sql[key] = pd.to_datetime(df_sql[key])
                                df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d %H:%M')

                            elif column_data_types[key]=="date" :
                                df_xml[key] = pd.to_datetime(df_xml[key])
                                df_xml[key] = df_xml[key].dt.strftime('%Y-%m-%d')

                                df_sql[key] = pd.to_datetime(df_sql[key])
                                df_sql[key] = df_sql[key].dt.str    

                            elif column_data_types[key]== 'bit':
                            
                                df_sql[key] = df_sql[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                                df_xml[key] = df_xml[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                       
                            elif column_data_types[key] in ['nvarchar', 'int', 'int64', 'bigint']:
                            

                            
                                df_xml[key] = df_xml[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                                df_sql[key] = df_sql[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)

                                
                    #null-values and str type conversion
                    df_xml = df_xml.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
                    df_sql = df_sql.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
                    df_sql = df_sql.fillna(value='Null')
                    df_xml = df_xml.fillna(value='Null')
                    df_sql_selected = df_sql[columns_selected]
  
                    df_xml_selected = df_xml[columns_selected]
                    df_xml_selected =df_xml_selected.astype(str)
                    df_sql_selected = df_sql_selected.astype(str)


                    # comparison = df_sql.equals(df_xml_selected)
                    comparison = df_xml_selected.equals(df_sql_selected)

                    unmatched_rows_sql = df_sql[~df_sql_selected.isin(df_xml_selected)].dropna(how='all')
                    for col in primary_keys_table_sql:
                        unmatched_rows_sql[col] = df_sql[col]
                    unmatched_rows_sql.index = unmatched_rows_sql.index + 1  
                    unmatched_rows_sql = unmatched_rows_sql[~unmatched_rows_sql.isin(primary_keys_table_sql)].dropna(how='all')
                

                
                    primary_keys_file  =  [col for col in primary_keys_table_sql if col in df_xml.columns]
                    if primary_keys_file:

                        unmatched_rows_xml = df_xml[~df_xml_selected.isin(df_sql_selected)].dropna(how='all')
                        for col in primary_keys_file:
                            unmatched_rows_xml[col] = df_xml[col] 
                        unmatched_rows_xml.index = unmatched_rows_xml.index + 1  
                        unmatched_rows_xml = unmatched_rows_xml[~unmatched_rows_xml.isin(primary_keys_file)].dropna(how='all')


                    count_unmatched_xml = len(unmatched_rows_xml)
                    count_unmatched_sql = len(unmatched_rows_sql)
                    total_rows_xml = len(df_xml)
                    total_rows_sql = len(df_sql)

                    count_unmatched_xml = 0
                    count_unmatched_sql = 0

                    if not comparison:
                        count_unmatched_tables_source += 1
                        count_unmatched_tables_target += 1

                        count_unmatched_xml =len(unmatched_rows_xml.index)
                        count_unmatched_sql =len(unmatched_rows_sql.index)


                    
                    # Set background color for unmatched rows to red and remaining cells to green
                    unmatched_rows_xml_html = unmatched_rows_xml.to_html(na_rep='-', escape=False)
                    unmatched_rows_xml_html = unmatched_rows_xml_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_xml_html = unmatched_rows_xml_html.replace('<tr>', '<tr style="text-align: center">')

                    
                    # Iterate over the DataFrame to set red background for non-matching cells
                    for index, row in unmatched_rows_xml.iterrows():
                        for col, value in row.items():
                        
                        
                                if col in primary_keys_file:
                                    unmatched_rows_xml_html = unmatched_rows_xml_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                                elif ( value != '-' and col not in primary_keys_file):
                                    unmatched_rows_xml_html = unmatched_rows_xml_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)




                    unmatched_rows_sql_html = unmatched_rows_sql.to_html(na_rep='-', escape=False)
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<tr>', '<tr style="text-align: center">')

                    for index, row in unmatched_rows_sql.iterrows():
                                for col, value in row.items():
                                
                                        if col in primary_keys_table_sql:
                                            unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: lightgreen">{value}</td>',1)
                                        elif ( value != '-' and col not in primary_keys_table_sql):
                                            unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

                    html_reports.append({
                        'table_name1': table_name1,
                        'table_name2': table_name2,
                        'comparison_result': comparison,
                        'unmatched_rows_source_html': unmatched_rows_xml_html,
                        'unmatched_rows_target_html': unmatched_rows_sql_html,
                        'total_rows_source': total_rows_xml,
                        'total_rows_target': total_rows_sql,
                        'count_unmatched_source': count_unmatched_xml,
                        'count_unmatched_target': count_unmatched_sql,
                        'compare_url': url_for('compare', table1=table_name1, table2=table_name2)


                    })

                context = {'reports': html_reports,
                            'count_tables_source': count_tables_source,
                            'count_tables_target': count_tables_target,
                            'count_unmatched_tables_source': count_unmatched_tables_source,
                            'count_unmatched_tables_target': count_unmatched_tables_target
                        }
                return render_template('Multiple.html', **context)
            



    ###############----------------- User Defined Query  for Synapse and Synapse --------------------###############################



    elif(validation_type =="user_defined"):
         
        if (database == "synapse" and file_input == "synapse" ):

            def connection(data):
                connection_dict = {}
                for index, row in data.iterrows():
                    connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']

                for key in connection_dict:
                    if key == "server":
                        server = connection_dict[key]
                    elif key =="database":
                        database = connection_dict[key]
                    elif key =="username":
                        username = connection_dict[key]

                driver= '{ODBC Driver 17 for SQL Server}'

                connection = (
                    f'DRIVER={driver};'
                    f'SERVER={server};'
                    f'DATABASE={database};'
                    f'Trusted_Connection=no;'
                    f'Authentication=ActiveDirectoryInteractive;'
                    f'MFA=Required;'
                    f'UID={username};'
                )
                return connection

            data_1 = pd.read_excel('C:\\TNDV\\User_defined\\Synapse_Connection.xlsx', sheet_name='Connection-1')
            data_2 = pd.read_excel('C:\\TNDV\\User_defined\\Synapse_Connection.xlsx', sheet_name='Connection-2')

            connection_1 = connection(data_1)
            connection_2 = connection(data_2)

            conn1 = pyodbc.connect(connection_1)
            conn2 = pyodbc.connect(connection_2)

            def get_dataframe_after_query(connection, user_query):
                result = pd.read_sql(user_query, connection)
                return result

            # user_query_1 = input("Enter your SQL query: ")
            # user_query_2 = input("Enter your SQL query: ")

            user_query_1 = user_query_source
            user_query_2 = user_query_target



            df_sql_1 = get_dataframe_after_query(conn1, user_query_1)
            df_sql_2 = get_dataframe_after_query(conn2, user_query_2)

            df_sql_1 = df_sql_1.astype(str)
            df_sql_2 = df_sql_2.astype(str)

            comparison = df_sql_1.equals(df_sql_2)
            unmatched_rows_sql_1 = df_sql_1[~df_sql_1.isin(df_sql_2)].dropna(how='all').fillna("-")
            unmatched_rows_sql_1.index = unmatched_rows_sql_1.index + 1  
            print(unmatched_rows_sql_1)

            unmatched_rows_sql_2 = df_sql_2[~df_sql_2.isin(df_sql_1)].dropna(how='all').fillna("-")
            unmatched_rows_sql_2.index = unmatched_rows_sql_2.index +1
            print(unmatched_rows_sql_2)

            count_unmatched_sql_1 = len(unmatched_rows_sql_1)
            count_unmatched_sql_2 = len(unmatched_rows_sql_2)
            total_rows_sql_1= len(df_sql_1)
            total_rows_sql_2 = len(df_sql_2)



            html_report = '<div class="report-container">'

            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source table completely matches the data in the Target table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source table  does not matches the data in the Target table</div>'



            #  Left column
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p>Source Count: {}</p>'.format(total_rows_sql_1)
            html_report += '<p>Target Count: {}</p>'.format(total_rows_sql_2)
            html_report += '</div>'
            
             # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_sql_1)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_sql_2)
            html_report += '</div>'
            html_report += '</div>'

         

            # Set background color for unmatched rows to red and remaining cells to green
            unmatched_rows_sql_1_html = unmatched_rows_sql_1.to_html(na_rep='-', escape=False)
            unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
            unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace('<tr>', '<tr style="text-align: center">')

            for index, row in unmatched_rows_sql_1.iterrows():
                for col, value in row.items():
                    if ( value != '-'):
                        unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

            

            unmatched_rows_sql_2_html =  unmatched_rows_sql_2.to_html(na_rep='-', escape=False)
            unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
            unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace('<tr>', '<tr style="text-align: center">')

            for index, row in unmatched_rows_sql_2.iterrows():
                for col, value in row.items():
                   
                    if ( value != '-'):
                        unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

            html_report += '<div class="container">'

            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_sql_1_html
            html_report += '</div>'

            
            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_sql_2_html
            html_report += '</div>'
            html_report += '</div>'

            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

            context = {
                'data': {
                    'comparison_result': comparison,  # Replace comparison_result with the actual comparison result variable
                },
                'report': html_report,
            }
            print(context['data']['comparison_result'])
            print("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk")
            print(context['report'])
         
            return render_template('user_defined_output.html', **context)



        ###############----------------- User Defined Query for Snowflake and Synapse --------------------###############################



        
        # elif ( file_input == "snowflake" ):

            
        #     def snowflake_connection(data_1):
                
        #         connection_dict_1 = {}
        #         for index, row in data_1.iterrows():

        #             connection_dict_1[row['Connection_Fields'].strip()] = row['Connection_Details']
        #         print(connection_dict_1)
        #         for key in connection_dict_1:
        #             print(key)
        #             if key == "account":
        #                 account = connection_dict_1[key]
        #                 print(account)
        #             elif key =="warehouse":
        #                 warehouse = connection_dict_1[key]
        #             elif key =="database":
        #                 database_snowflake = connection_dict_1[key]

        #             elif key =="schema":
        #                 schema = connection_dict_1[key]
                    
        #             elif key =="username":
        #                 username = connection_dict_1[key]


                
        #         connection_snowflake = snowflake.connector.connect(
        #             account=account,
        #             warehouse=warehouse,
        #             database=database_snowflake,
        #             schema=schema,
        #             user=username,
        #             password=source_password
        #         )
        #         return connection_snowflake
        #     if(database == "synapse"):
        #         data_2 = pd.read_excel('C:\\TNDV\\User_defined\\Synapse_Snowflake_Connection.xlsx', sheet_name='synapse_connection')
        #         data_1 = pd.read_excel('C:\\TNDV\\User_defined\\Synapse_Snowflake_Connection.xlsx', sheet_name='snowflake_connection')
        #         print(data_1)
        #         connection_snowflake=snowflake_connection(data_1)
        #         print("entered inside the code")
                


        #         connection_dict_2 = {}
        #         for index, row in data_2.iterrows():
        #                 connection_dict_2[row['Connection_Fields'].strip()] = row['Connection_Details']

        #         for key in connection_dict_2:
        #             if key == "server":
        #                 server = connection_dict_2[key]
        #             elif key =="database":
        #                 database = connection_dict_2[key]
        #             elif key =="username":
        #                 username = connection_dict_2[key]

        #         driver= '{ODBC Driver 17 for SQL Server}'

        #         connection = (
        #             f'DRIVER={driver};'
        #             f'SERVER={server};'
        #             f'DATABASE={database};'
        #             f'Trusted_Connection=no;'
        #             f'Authentication=ActiveDirectoryInteractive;'
        #             f'MFA=Required;'
        #             f'UID={username};'
        #         )

        #         print(user_query_source)
        #         print(user_query_target)


        #         connection_synapse = pyodbc.connect(connection)
        #         user_query_1 = user_query_source
        #         user_query_2 = user_query_target


        #         print("Sachinnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn")
        #         print(user_query_1)
        #         print(user_query_2)

        #         df_sql_1 = pd.read_sql(user_query_1, connection_snowflake)
        #         df_sql_2 = pd.read_sql(user_query_2, connection_synapse)
        #         df_sql_2.columns = df_sql_2.columns.str.upper()
        #         df_sql_1 = df_sql_1.astype(str)
        #         df_sql_2 = df_sql_2.astype(str)
        elif ( file_input == "snowflake" ):
 
            data_1 = pd.read_excel('C:\\TNDV\\User_defined\\Synapse_Snowflake_Connection.xlsx', sheet_name='snowflake_connection')
            connection_dict_1 = {}
            for index, row in data_1.iterrows():
 
                connection_dict_1[row['Connection_Fields'].strip()] = row['Connection_Details']
            print(connection_dict_1)
            for key in connection_dict_1:
                print(key)
                if key == "account":
                    account = connection_dict_1[key]
                    print(account)
                elif key =="warehouse":
                    warehouse = connection_dict_1[key]
                elif key =="database":
                    database_snowflake = connection_dict_1[key]
 
                elif key =="schema":
                    schema = connection_dict_1[key]
               
                elif key =="username":
                    username = connection_dict_1[key]
 
 
           
            connection_snowflake = snowflake.connector.connect(
                account=account,
                warehouse=warehouse,
                database=database_snowflake,
                schema=schema,
                user=username,
                password=source_password
            )
 
            if(database == "synapse"):
                print("entered inside the code")
                data_2 = pd.read_excel('C:\\TNDV\\User_defined\\Synapse_Snowflake_Connection.xlsx', sheet_name='synapse_connection')
 
 
                connection_dict_2 = {}
                for index, row in data_2.iterrows():
                        connection_dict_2[row['Connection_Fields'].strip()] = row['Connection_Details']
 
                for key in connection_dict_2:
                    if key == "server":
                        server = connection_dict_2[key]
                    elif key =="database":
                        database = connection_dict_2[key]
                    elif key =="username":
                        username = connection_dict_2[key]
 
                driver= '{ODBC Driver 17 for SQL Server}'
 
                connection = (
                    f'DRIVER={driver};'
                    f'SERVER={server};'
                    f'DATABASE={database};'
                    f'Trusted_Connection=no;'
                    f'Authentication=ActiveDirectoryInteractive;'
                    f'MFA=Required;'
                    f'UID={username};'
                )
 
                print(user_query_source)
                print(user_query_target)
 
 
                connection_synapse = pyodbc.connect(connection)
                user_query_1 = user_query_source
                user_query_2 = user_query_target
 
 
                print("Sachinnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn")
                print(user_query_1)
                print(user_query_2)
 
                df_sql_1 = pd.read_sql(user_query_1, connection_snowflake)
                df_sql_2 = pd.read_sql(user_query_2, connection_synapse)
                df_sql_2.columns = df_sql_2.columns.str.upper()
                df_sql_1 = df_sql_1.astype(str)
                df_sql_2 = df_sql_2.astype(str)


                comparison = df_sql_1.equals(df_sql_2)
                unmatched_rows_sql_1 = df_sql_1[~df_sql_1.isin(df_sql_2)].dropna(how='all').fillna("-")
                unmatched_rows_sql_1.index = unmatched_rows_sql_1.index + 1  
        
                unmatched_rows_sql_2 = df_sql_2[~df_sql_2.isin(df_sql_1)].dropna(how='all').fillna("-")
                unmatched_rows_sql_2.index = unmatched_rows_sql_2.index +1

                count_unmatched_sql_1 = len(unmatched_rows_sql_1)
                count_unmatched_sql_2 = len(unmatched_rows_sql_2)
                total_rows_sql_1= len(df_sql_1)
                total_rows_sql_2 = len(df_sql_2)

                html_report = '<div class="report-container">'

                # Comparison result
                if comparison:
                    html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source table completely matches the data in the Target table</div>'
                else:
                    html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source table  does not matches the data in the Target table</div>'



                #  Left column
                html_report += '<div style="display: flex;">'
                html_report += '<div class="left-column">'
                html_report += '<h3>Table Count:</h3>'
                html_report += '<p>Source Count: {}</p>'.format(total_rows_sql_1)
                html_report += '<p>Target Count: {}</p>'.format(total_rows_sql_2)
                html_report += '</div>'
                
                # Right column
                html_report += '<div style="flex: 1;">'
                html_report += '<h4>Unmatched Data Count:</h4>'
                html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_sql_1)
                html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_sql_2)
                html_report += '</div>'
                html_report += '</div>'

            

                # Set background color for unmatched rows to red and remaining cells to green
                unmatched_rows_sql_1_html = unmatched_rows_sql_1.to_html(na_rep='-', escape=False)
                unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace('<tr>', '<tr style="text-align: center">')

                for index, row in unmatched_rows_sql_1.iterrows():
                    for col, value in row.items():
                        if ( value != '-'):
                            unmatched_rows_sql_1_html = unmatched_rows_sql_1_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

                

                unmatched_rows_sql_2_html =  unmatched_rows_sql_2.to_html(na_rep='-', escape=False)
                unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
                unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace('<tr>', '<tr style="text-align: center">')

                for index, row in unmatched_rows_sql_2.iterrows():
                    for col, value in row.items():
                    
                        if ( value != '-'):
                            unmatched_rows_sql_2_html = unmatched_rows_sql_2_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

                html_report += '<div class="container">'

                html_report += '<div class="left-table">'
                html_report += '<h4>Unmatched data from Source:</h4>'
                html_report += unmatched_rows_sql_1_html
                html_report += '</div>'

                
                html_report += '<div class="right-table">'
                html_report += '<h4>Unmatched data from Target:</h4>'
                html_report += unmatched_rows_sql_2_html
                html_report += '</div>'
                html_report += '</div>'


                context = {
                    'data': {
                        'comparison_result': comparison,  
                    },
                    'report': html_report,
                }
             
                return render_template('user_defined_output.html', **context)
            # elif(database =="SQL_Server"):
            #     data_1 = pd.read_excel('C:\\TNDV\\User_defined\\Synapse_Connection.xlsx', sheet_name='Snowflake_Connection')
            #     connection_snowflake=snowflake_connection(data1)
            #     data_2 = pd.read_excel('C:\TNDV\Schema\SQL_Server_Connection.xlsx',sheet_name='SQL_Server_Connection-2')
             
            #     connection_dict = {}
            #     for index, row in data_2.iterrows():
            #         connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
               
            #     for key in connection_dict:
                   
            #         if key == "server":
            #             server_sq = connection_dict[key]
                        
            #         elif key =="database":
            #             database_sq = connection_dict[key]
                        
            #         elif key =="username":
            #             username_sq = connection_dict[key]


            #     password_sq = password
            #     conn1 = pyodbc.connect(
            #             'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server_sq + ';DATABASE=' + database_sq + ';UID=' + username_sq + ';PWD=' + password_sq
            #         )


@app.route('/compare')
def compare():
    # Define the context for rendering the template
    
    
    table_name1 = request.args.get('table1')  # Get the value of the 'table1' query parameter
    table_name2 = request.args.get('table2')
   
    filtered_list = list(filter(lambda html_report:  html_report['table_name1'] == table_name1 and html_report['table_name2'] ==table_name2, html_reports))
    context = {'reports': filtered_list}  # Make sure 'html_reports' is defined appropriately

  
    # Render the compare.html template with the given context
    return render_template('compare.html', **context)



@app.route('/compare_pass')
def compare_pass():
    # Define the context for rendering the template
    
    
    table_name1 = request.args.get('table1')  # Get the value of the 'table1' query parameter
    table_name2 = request.args.get('table2')
   
    filtered_list = list(filter(lambda html_report:  html_report['table_name1'] == table_name1 and html_report['table_name2'] ==table_name2, html_reports))
    context = {'reports': filtered_list}  # Make sure 'html_reports' is defined appropriately

  
    # Render the compare.html template with the given context
    return render_template('compare_pass.html', **context)


@app.route('/Key_Matching')
def Key_Matching():
    schema1_name = unmatched_reports[0]['schema1_name']
    table_name1 = unmatched_reports[0]['table_name1']
    context = {'schema1_name': schema1_name, 'table_name1': table_name1}
    return render_template('Key_Matching.html', **context)


if __name__ == '__main__':
    app.run(port=1000)

    
   

   