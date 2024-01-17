import csv
from flask import Flask, app, render_template, request,Response,jsonify,url_for

import json
import pandas as pd
from pandasql import sqldf
import xml.etree.ElementTree as ET
import snowflake.connector
import pyodbc
import numpy as np




def generate_report(file_input,file_name,db,table,password,source_password):

    file_input= file_input
    file_name =file_name
    database = db
    table = table
    password = password
    source_password = source_password
   
    print("rrrrrrrrrrrrrrrrrrrrrrrrrrrrrr")
    

       ############################SNOWFLAKE AS TARGET####################################################
    if (database == "Snowflake"):
        data = pd.read_excel('C:\TNDV\Single_Table\Single_Table_Connection.xlsx',sheet_name='Snowflake_Connection')
        
    
        connection_dict = {}
        for index, row in data.iterrows():
            connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
     
     
        for key in connection_dict:
        
        
            if key == "account":
                account = connection_dict[key]
            elif key =="warehouse":
                warehouse = connection_dict[key]
            elif key =="database":
                database = connection_dict[key]
            elif key =="schema":
                schema = connection_dict[key]
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

        def get_primary_keys_snowflake(connection, schema1, table_name):
            cursor = connection.cursor()
            query = f"""
                SHOW PRIMARY KEYS IN TABLE {schema1}.{table_name}
            """
            cursor.execute(query)
            primary_keys = [row[4] for row in cursor.fetchall()]
            
            return primary_keys
    
        primary_keys_table_snowflake = get_primary_keys_snowflake(conn, schema, table)
   
    ###############-----------------SNOWFLAKE VS XML--------------------###############################
        if (file_input== "xml"):
            
            csv_file_path = "C:\\TNDV\\Single_Table\\Folder_Path_SingleTable.csv"

            df_csv = pd.read_csv(csv_file_path)
            # Loop through each row in the CSV

            for index, row in df_csv.iterrows():

                xml_path = row['xml_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path

            xml_file = xml_path + "\\" + file_name + ".xml"# Modify 'file_name' as needed
            
            tree = ET.parse(xml_file)
            root = tree.getroot()
            xml_data = []
            for child in root:
                row = {}
                for subchild in child:
                    row[subchild.tag.upper()] = subchild.text
                    # row[subchild.tag] = subchild.text
                xml_data.append(row)
            df_xml = pd.DataFrame(xml_data)
            # Fetch data from Snowflake table
            query = f"SELECT * FROM {table}"
            df_snowflake = pd.read_sql(query, conn)

            def get_column_data_types():
                column_data_types = {}
                query = f"SHOW COLUMNS IN TABLE {table}"
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
         
            df_snowflake = df_snowflake[df_xml.columns]
            columns_selected=[]
            dict_selected ={}

            for key in column_data_types_dict:
                if key in df_xml.columns:
                    columns_selected.append(key)

            for key in column_data_types_dict:
                if key in columns_selected:
                    if (column_data_types_dict[key]== 'TIMESTAMP_NTZ'):
                    
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

            #Total Count
            count_unmatched_xml = len(unmatched_rows_xml)
            count_unmatched_snowflake = len(unmatched_rows_snowflake)
            total_rows_xml = len(df_xml)
            total_rows_snowflake = len(df_snowflake)

           
            html_report = '<div class="report-container">'
            # html_report += '<h1 class="report-title">Comparison Results</h1>'

            #  Left column
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p> Source Count: {}</p>'.format(total_rows_snowflake)
            html_report += '<p> Target Count: {}</p>'.format(total_rows_xml)
            html_report += '</div>'

                    # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_xml)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_snowflake)
            html_report += '</div>'
            html_report += '</div>'

            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source file completely matches the data in the  Target table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source file does not match the data in the Target table</div>'

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

            
            html_report += '<div class="container">'

            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_xml_html
            html_report += '</div>'

            
            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_snowflake_html
            html_report += '</div>'
            html_report += '</div>'

     

            context = {
                'data': {
                    'comparison_result': comparison,  # Replace comparison_result with the actual comparison result variable
                },
                'report': html_report,
            }
            # return Response(json.dumps(context))
            

        

    ###############-----------------SNOWFLAKE VS JSON--------------------###############################
        elif (file_input== "json"):
            


            csv_file_path = "C:\\TNDV\\Single_Table\\Folder_Path_SingleTable.csv"
            df_csv = pd.read_csv(csv_file_path)

            # Loop through each row in the CSV
            for index, row in df_csv.iterrows():
                json_path1 = row['json_path'].strip('"')  # Assuming 'json_path' is the column containing the JSON path
            json_file = json_path1 + "\\" + file_name + ".json"  # Modify 'file_name' as needed


            data_dict = pd.read_json(json_file)
            df_json = pd.DataFrame(data_dict)
            df_json.columns = df_json.columns.str.upper()
            query = f"SELECT * FROM {table}"
            df_snowflake = pd.read_sql(query, conn)


            def get_column_data_types():
                column_data_types = {}
                query = f"SHOW COLUMNS IN TABLE {table}"
                cursor = conn.cursor()
                cursor.execute(query)
                for row in cursor:
                   
                    column_name = row[2]  # Column name is in the second position
                    data_type = row[3]    # Data type is in the third position
                   
                    dict_data = json.loads(data_type)
                    for data in dict_data:
                        data=dict_data['type']
                    column_data_types[column_name] = data
                    
                return column_data_types

            column_data_types_dict = get_column_data_types()
          
            df_snowflake = df_snowflake[df_json.columns]
            columns_selected=[]
            dict_selected ={}

            for key in column_data_types_dict:
                if key in df_json.columns:
                    columns_selected.append(key)

            for key in column_data_types_dict:
                if key in columns_selected:
                    if (column_data_types_dict[key]== 'TIMESTAMP_NTZ'):
                       
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

            #Total Count
            count_unmatched_json = len(unmatched_rows_json)
            count_unmatched_snowflake = len(unmatched_rows_snowflake)
            total_rows_json = len(df_json)
            total_rows_snowflake = len(df_snowflake)
            print(unmatched_rows_json)
            print(unmatched_rows_snowflake)

            html_report = '<div class="report-container">'
     
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p>Source Count: {}</p>'.format(total_rows_json)
            html_report += '<p>Target Count: {}</p>'.format(total_rows_snowflake)
            html_report += '</div>'
            
            # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_json)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_snowflake)
            html_report += '</div>'
            html_report += '</div>'


            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source file completely matches the data in the  Target table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source file does not match the data in the Target table</div>'

       
            
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

            html_report += '<div class="container">'

            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_json_html
            html_report += '</div>'

            
            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_snowflake_html
            html_report += '</div>'
            html_report += '</div>'
                


            context = {
                'data': {
                    'comparison_result': comparison,  # Replace comparison_result with the actual comparison result variable
                },
                'report': html_report,
            }
      
    

    ###############-----------------SNOWFLAKE VS CSV--------------------###############################
        elif (file_input== "csv"):
           
            csv_file_path = "C:\\TNDV\\Single_Table\\Folder_Path_SingleTable.csv"
            df_csv = pd.read_csv(csv_file_path)
            
            # Loop through each row in the CSV
            for index, row in df_csv.iterrows():
                #json_path1 = row['json_path'].strip('"')  # Assuming 'json_path' is the column containing the JSON path
                csv_path = row['csv_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path
                
            # Use json_path in your code to construct the JSON file path
            csv_file = csv_path + "\\" + file_name + ".csv" 
            df_csv = pd.read_csv(csv_file,dtype=str)
           
            df_csv.columns = df_csv.columns.str.upper()

            query = f"SELECT * FROM {table}"
            df_snowflake = pd.read_sql(query, conn)

            def get_column_data_types():
                column_data_types = {}
                query = f"SHOW COLUMNS IN TABLE {table}"
                cursor = conn.cursor()
                cursor.execute(query)
                for row in cursor:
             
                    column_name = row[2]  # Column name is in the second position
                    data_type = row[3]    # Data type is in the third position
               
                    dict_data = json.loads(data_type)
                    for data in dict_data:
                        data=dict_data['type']
                    column_data_types[column_name] = data
                    
                return column_data_types

            column_data_types_dict = get_column_data_types()


            df_snowflake = df_snowflake[df_csv.columns]
            columns_selected=[]
            dict_selected ={}
            for key in column_data_types_dict:
                if key in df_csv.columns:
                    columns_selected.append(key)

            df_csv=df_csv[columns_selected]
            df_snowflake = df_snowflake[columns_selected]

            print(column_data_types_dict,"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
          
            for key in column_data_types_dict:
                  
                    if key in columns_selected:
                     
                        if (column_data_types_dict[key]== "TIMESTAMP_NTZ"):

                            df_csv[key] = pd.to_datetime(df_csv[key])

                            df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d %H:%M')
                            df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                            df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d %H:%M')

                        elif (column_data_types_dict[key]=="DATE"):
                            df_csv[key] = pd.to_datetime(df_csv[key])
                            df_csv[key] = df_csv[key].dt.strftime('%Y-%m-%d')
                            df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                            df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d')

                        
                        elif (column_data_types_dict[key]== "BOOLEAN"):
                            df_snowflake[key] = df_snowflake[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                            df_csv[key] = df_csv[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                        elif (column_data_types_dict[key]== "TEXT"):
                            if df_csv[key].dtype == 'float64' or df_csv[key].dtype == 'int64' :
                                df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)

                            


                        elif (column_data_types_dict[key]== "REAL"):
               
                            df_snowflake[key] = df_snowflake[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                            df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                            
                        elif (column_data_types_dict[key]== "FIXED"):
               
                            df_snowflake[key] = df_snowflake[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                            df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                        



            df_csv = df_csv.replace(['nan', 'None', 'Null', 'NaN','NUL','NULL', ''], '')
            print()
            df_snowflake = df_snowflake.replace(['nan', 'None', 'Null','NUL' ,'NaN','NULL', ''], '')
            df_snowflake = df_snowflake.fillna(value='')
            df_csv = df_csv.fillna(value='')
            df_snowflake_selected = df_snowflake
           
            df_csv_selected = df_csv
            df_csv = df_csv.astype(str)
            df_snowflake = df_snowflake.astype(str)
            comparison = df_csv.equals(df_snowflake)


            print("*****************************************")

            unmatched_rows_snowflake = df_snowflake[~df_snowflake.isin(df_csv)].dropna(how='all').fillna("-")
            unmatched_rows_snowflake.index = unmatched_rows_snowflake.index + 1  
            print(unmatched_rows_snowflake)

            unmatched_rows_csv = df_csv[~df_csv.isin(df_snowflake)].dropna(how='all').fillna("-")
            unmatched_rows_csv.index = unmatched_rows_csv.index +1
            print(unmatched_rows_csv)


            # Total Count
            count_unmatched_csv = len(unmatched_rows_csv)
            count_unmatched_snowflake = len(unmatched_rows_snowflake)
            total_rows_CSV = len(df_csv)
            total_rows_snowflake = len(df_snowflake)

            html_report = '<div class="report-container">'
        

            #  Left column
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p>Source Count: {}</p>'.format(total_rows_snowflake)
            html_report += '<p>Target Count: {}</p>'.format(total_rows_CSV)
            html_report += '</div>'
            
             # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_csv)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_snowflake)
            html_report += '</div>'
            html_report += '</div>'

            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the CSV file completely matches the data in the Snowflake Server table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the CSV file does not match the data in the Snowflake Server table</div>'


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

            html_report += '<div class="container">'

            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_csv_html
            html_report += '</div>'

            
            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_snowflake_html
            html_report += '</div>'
            html_report += '</div>'

            context = {
                'data': {
                    'comparison_result': comparison,  # Replace comparison_result with the actual comparison result variable
                },
                'report': html_report,
            }

    

        ##############-----------------Snowflake VS FlatFile--------------------###############################
        elif (file_input == "FlatFile"):



            csv_file_path = "C:\\TNDV\\Single_Table\\Folder_Path_SingleTable.csv"
            df_csv = pd.read_csv(csv_file_path)
            
            # Loop through each row in the CSV
            for index, row in df_csv.iterrows():
                #json_path1 = row['json_path'].strip('"')  # Assuming 'json_path' is the column containing the JSON path
                flat_path = row['flat_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path
                
            # Use json_path in your code to construct the JSON file path
            flat_file = flat_path + "\\" + file_name + ".txt" # Modify 'file_name' as needed

            #determining the delimiter of the flat file
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
            query = f"SELECT * FROM {table}"
            df_snowflake = pd.read_sql(query, conn)


            def get_column_data_types():
                column_data_types = {}
                query = f"SHOW COLUMNS IN TABLE {table}"
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
            separator = determine_delimiter(flat_file)

            # Read the flat file into a DataFrame with the determined delimiter
            if separator:
                df_csv = pd.read_csv(flat_file, delimiter=separator)
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
            unmatched_rows_snowflake.index = unmatched_rows_snowflake.index + 1  
            unmatched_rows_snowflake = unmatched_rows_snowflake[~unmatched_rows_snowflake.isin(primary_keys_table_snowflake)].dropna(how='all')
            

            primary_keys_file  =  [col for col in primary_keys_table_snowflake if col in df_csv.columns]
            if primary_keys_file:
                unmatched_rows_csv = df_csv[~df_csv.isin(df_snowflake)].dropna(how='all')
        
                for col in primary_keys_file:
                    unmatched_rows_csv[col] = df_csv[col]  # Perform operations on the specific column
                unmatched_rows_csv.index = unmatched_rows_csv.index +1
                unmatched_rows_csv = unmatched_rows_csv[~unmatched_rows_csv.isin(primary_keys_file)].dropna(how='all')



            #Total Count
            count_unmatched_csv = len(unmatched_rows_csv)
            count_unmatched_snowflake = len(unmatched_rows_snowflake)
            total_rows_csv = len(df_csv)
            total_rows_snowflake = len(df_snowflake)

            html_report = '<div class="report-container">'
           
            #  Left column
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p>Source Count: {}</p>'.format(total_rows_snowflake)
            html_report += '<p>Target Count: {}</p>'.format(total_rows_csv)
            html_report += '</div>'
            
            # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_csv)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_snowflake)
            html_report += '</div>'
            html_report += '</div>'


            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source file completely matches the data in the  Target table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source file does not match the data in the Target table</div>'

       

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

            html_report += '<div class="container">'
            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_csv_html
            html_report += '</div>'

            
            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_snowflake_html
            html_report += '</div>'
            html_report += '</div>'

            context = {
                'data': {
                    'comparison_result': comparison, 
                },
                'report': html_report,
            }



    ###############-----------------SNOWFLAKE VS SQL_Server --------------------###############################
        elif (file_input == "SQL_Server"):

            data = pd.read_excel('C:\TNDV\Single_Table\Single_Table_Connection.xlsx',sheet_name='SQL_Server_Connection')
        
            connection_dict = {}
            for index, row in data.iterrows():
                connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
      
            for key in connection_dict:
          
                if key == "server":
                    server_sq = connection_dict[key]
                  
                elif key =="database":
                    database_sq = connection_dict[key]
                    
                elif key =="username":
                    username_sq = connection_dict[key]
                  
            password_sq = source_password
    

            conn_sql = pyodbc.connect(
                    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server_sq + ';DATABASE=' + database_sq + ';UID=' + username_sq + ';PWD=' + password_sq
                )
    

   
            
            table_sql = file_name
            table_snowflake = table

            def get_primary_keys_sql_source(connection, table_name):
                table_name = table_sql.split('.')[1]
                print(table_name,"+++++++++++++++++++++++++++++++++++++++++++++++++")
                query = """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
                    AND TABLE_NAME = ?
                """
                primary_keys_df = pd.read_sql(query, connection, params=[table_name])
            
                return primary_keys_df['COLUMN_NAME'].tolist()

            primary_keys_table_sql_source = get_primary_keys_sql_source(conn_sql, table_sql)
            query_snowflake = f"SELECT * FROM {table_snowflake}"
            df_snowflake = pd.read_sql(query_snowflake, conn)


           

            # Fetch data from SQL Server table
            query_sql = f"SELECT * FROM {table_sql}"
            df_sql = pd.read_sql(query_sql, conn_sql)
            df_sql.columns = df_sql.columns.str.upper()

            # Snowflake column data types
            def get_column_data_types_snowflake():
                column_data_types_snowflake = {}
                query_snowflake = f"SHOW COLUMNS IN TABLE {table_snowflake}"
                cursor_snowflake = conn.cursor()
                cursor_snowflake.execute(query_snowflake)
                for row in cursor_snowflake:
                    column_name = row[2]
                    data_type = row[3]
                    dict_data = json.loads(data_type)
                    for data in dict_data:
                        data = dict_data['type']
                    column_data_types_snowflake[column_name] = data
                return column_data_types_snowflake

            column_data_types_snowflake_dict = get_column_data_types_snowflake()

            # SQL Server column data types
            def get_column_data_types_sql():
                column_data_types_sql = {}
                query_sql = f"SELECT * FROM {table_sql}"
                cursor_sql = conn_sql.cursor()
                data = cursor_sql.execute(query_sql)
                for column in data.description:
                    column_name = column[0]
                    data_type = str(column[1])
                    column_data_types_sql[column_name] = data_type
                return column_data_types_sql

            column_data_types_sql_dict = get_column_data_types_sql()

            # Select common columns between Snowflake and SQL Server tables
            columns_selected = [col for col in df_sql.columns if col in df_snowflake.columns]


            # Convert Snowflake table columns to appropriate data types
            for key in column_data_types_snowflake_dict:
                if key in columns_selected:
                    if column_data_types_snowflake_dict[key] == 'TIMESTAMP_NTZ':
                        df_snowflake[key] = pd.to_datetime(df_snowflake[key], format='%Y-%m-%d %H:%M')
                    elif column_data_types_snowflake_dict[key] == 'BOOLEAN':
                        df_snowflake[key] = df_snowflake[key].replace({'True': True, 'False': False}).astype(bool)

                    elif column_data_types_snowflake_dict[key]=="DATE":
                        df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                        df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d')

                       
                        

            # Convert SQL Server table columns to appropriate data types
            for key in column_data_types_sql_dict:
                if key in columns_selected:
                    if column_data_types_sql_dict[key] == "<class 'datetime.datetime'>":
                        df_sql[key] = pd.to_datetime(df_sql[key], format='%Y-%m-%d %H:%M')
                    elif column_data_types_sql_dict[key] == "<class 'bool'>":
                        df_sql[key] = df_sql[key].replace({'True': True, 'False': False}).astype(bool)

                    elif column_data_types_sql_dict[key] == "<class 'date'>":
                        df_sql[key] = pd.to_datetime(df_sql[key])
                        df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d')




            replacement_value = 'None'
            df_snowflake.replace(['', np.nan], replacement_value, inplace=True)
            df_sql.replace(['', np.nan], replacement_value, inplace=True)

            df_snowflake = df_snowflake.astype(str)
            df_sql = df_sql.astype(str)
            comparison = df_snowflake[columns_selected].equals(df_sql[columns_selected])




            primary_keys_table_sql_source = [col.upper() for col in primary_keys_table_sql_source]
            unmatched_rows_sql = df_sql[~df_sql.isin(df_snowflake)].dropna(how='all')
            for col in primary_keys_table_sql_source:
                unmatched_rows_sql[col] = df_sql[col]
            unmatched_rows_sql.index = unmatched_rows_sql.index + 1  
            unmatched_rows_sql = unmatched_rows_sql[~unmatched_rows_sql.isin(primary_keys_table_sql_source)].dropna(how='all')



            unmatched_rows_snowflake = df_snowflake[~df_snowflake.isin(df_sql)].dropna(how='all')
            for col in primary_keys_table_snowflake:
                unmatched_rows_snowflake[col] = df_snowflake[col]
            unmatched_rows_snowflake.index = unmatched_rows_snowflake.index + 1  
            unmatched_rows_snowflake = unmatched_rows_snowflake[~unmatched_rows_snowflake.isin(primary_keys_table_snowflake)].dropna(how='all')

            

            #  Total Count
            count_unmatched_snowflake = len(unmatched_rows_snowflake)
            count_unmatched_sql = len(unmatched_rows_sql)
            total_rows_snowflake = len(df_snowflake)
            total_rows_sql = len(df_sql)
            

         

            html_report = '<div class="report-container">'
            # html_report += '<h1 class="report-title">Comparison Results</h1>'

            #  Left column
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p>Source Count: {}</p>'.format(total_rows_snowflake)
            html_report += '<p>Target Count: {}</p>'.format(total_rows_sql)
            html_report += '</div>'

            # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_snowflake)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_sql)
            html_report += '</div>'
            html_report += '</div>'


            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source file completely matches the data in the  Target table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source file does not match the data in the Target table</div>'

      
            unmatched_rows_sql_html = unmatched_rows_sql.to_html(na_rep='-', escape=False)
            unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
            unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<tr>', '<tr style="text-align: center">')

            for index, row in unmatched_rows_sql.iterrows():
                for col, value in row.items():
                    
                    if col in primary_keys_table_sql_source:
                                unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                    elif ( value != '-' and col not in primary_keys_table_sql_source):
                                unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

    

            unmatched_rows_snowflake_html =  unmatched_rows_snowflake.to_html(na_rep='-', escape=False)
            unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
            unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<tr>', '<tr style="text-align: center">')

            for index, row in unmatched_rows_snowflake.iterrows():
                for col, value in row.items():
                    
                    if col in primary_keys_table_snowflake:
                                unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                    elif ( value != '-' and col not in primary_keys_table_snowflake):
                                unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

    


            html_report += '<div class="container">'
            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_sql_html
            html_report += '</div>'

            
            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_snowflake_html
            html_report += '</div>'
            html_report += '</div>'


            context = {
                'data': {
                    'comparison_result': comparison,  # Replace comparison_result with the actual comparison result variable
                },
                'report': html_report,
            }

                




    ############################SSMS AS TARGET####################################################    
    elif (database == "SQL_Server"):


        data = pd.read_excel('C:\TNDV\Single_Table\Single_Table_Connection.xlsx',sheet_name='SQL_Server_Connection')
     
        connection_dict = {}
        for index, row in data.iterrows():
            connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']
       
       
        for key in connection_dict:
            if key == "server":
                server_sq = connection_dict[key]

            elif key =="database":
                database_sq = connection_dict[key]
              
            elif key =="username":
                username_sq = connection_dict[key]
           
        
            
        password_sq = password
        print(server_sq,database_sq,username_sq,password_sq)
        print(type(server_sq),type(database_sq),type(username_sq),type(password_sq))
        conn= pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server_sq + ';DATABASE=' + database_sq + ';UID=' + username_sq + ';PWD=' + password_sq
            )
        


        def get_primary_keys_sql(connection, table_name):
                    table_name = table.split('.')[1]
                    print(table_name,"+++++++++++++++++++++++++++++++++++++++++++++++++")
                    query = """
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
                        AND TABLE_NAME = ?
                    """
                    primary_keys_df = pd.read_sql(query, connection, params=[table_name])
               
                    return primary_keys_df['COLUMN_NAME'].tolist()
        

        primary_keys_table_sql = get_primary_keys_sql(conn, table)


    ###############-----------------SSMS VS CSV--------------------###############################
        if (file_input== "csv"):
      
            csv_file_path = "C:\\TNDV\\Single_Table\\Folder_Path_SingleTable.csv"
            df_csv = pd.read_csv(csv_file_path)
            
            # Loop through each row in the CSV
            for index, row in df_csv.iterrows():
                #json_path1 = row['json_path'].strip('"')  # Assuming 'json_path' is the column containing the JSON path
                csv_path = row['csv_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path
                
            # Use json_path in your code to construct the JSON file path
            csv_file = csv_path + "\\" + file_name + ".csv" # Modify 'file_name' as needed



            df_csv = pd.read_csv(csv_file, low_memory=False)
            query = f"SELECT * FROM {table}"
            df_sql = pd.read_sql(query, conn)
            cursor = conn.cursor()
            parts = table.split('.')
            if len(parts) == 2:
                schema, table = parts
            else:
                schema = 'dbo'  # Default schema if not provided

            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'")
            
            column_data_types = {row.COLUMN_NAME: row.DATA_TYPE for row in cursor.fetchall()}
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

                    elif column_data_types[key]== 'nvarchar':
                    

                        df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)



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

           

            count_unmatched_csv = len(unmatched_rows_csv)
            count_unmatched_sql = len(unmatched_rows_sql)
            total_rows_csv = len(df_csv_selected)
            total_rows_sql = len(df_sql_selected)

            html_report = '<div class="report-container">'
   
            #  Left column
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p>Source Count: {}</p>'.format(total_rows_csv)
            html_report += '<p>Target Count: {}</p>'.format(total_rows_sql)
            html_report += '</div>'

            # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_csv)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_sql)
            html_report += '</div>'
            html_report += '</div>'


            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source file completely matches the data in the  Target table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source file does not match the data in the Target table</div>'
            

           
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


            # Iterate over the DataFrame to set red background for non-matching cells
            html_report += '<div class="container">'

            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_csv_html
            html_report += '</div>'

            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_sql_html
            html_report += '</div>'
            html_report += '</div>'
         

            context = {
                'data': {
                    'comparison_result': comparison,  # Replace comparison_result with the actual comparison result variable
                },
                'report': html_report,
            }



    ###############-----------------SSMS VS XML--------------------###############################
        elif (file_input== "xml"):
 
            
            csv_file_path = "C:\\TNDV\\Single_Table\\Folder_Path_SingleTable.csv"

            df_csv = pd.read_csv(csv_file_path)
            # Loop through each row in the CSV

            for index, row in df_csv.iterrows():

                xml_path = row['xml_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path

            xml_file = xml_path + "\\" + file_name + ".xml"# Modify 'file_name' as needed
            print(xml_file,"^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
          
            tree = ET.parse(xml_file)
            root = tree.getroot()
            xml_data = []
            for child in root:
                row = {}
                for subchild in child:
                    # row[subchild.tag.upper()] = subchild.text
                    row[subchild.tag] = subchild.text
                xml_data.append(row)
            df_xml = pd.DataFrame(xml_data)

            query = f"SELECT * FROM {table}"
            df_sql = pd.read_sql(query, conn)



            cursor = conn.cursor()
            parts = table.split('.')
            if len(parts) == 2:
                schema, table = parts
            else:
                schema = 'dbo'  # Default schema if not provided

            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'")
            
            column_data_types = {row.COLUMN_NAME: row.DATA_TYPE for row in cursor.fetchall()}

            df_sql = df_sql[df_xml.columns]
            columns_selected=[]
    
            columns_selected = df_xml.columns

            for key in column_data_types:
                for key in columns_selected:
                    if column_data_types[key] == 'datetime2':
                 
        
                        df_xml[key] = pd.to_datetime(df_xml[key])
                        df_xml[key] = df_xml[key].dt.strftime('%Y-%m-%d %H:%M')
                        df_sql[key] = pd.to_datetime(df_sql[key])
                        df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d %H:%M')

                    elif column_data_types[key]=="date" :
                        df_xml[key] = pd.to_datetime(df_xml[key])
                        df_xml[key] = df_xml[key].dt.strftime('%Y-%m-%d')

                        df_sql[key] = pd.to_datetime(df_sql[key])
                        df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d')

                    
                    elif column_data_types[key]== 'bit':
                    
                        df_sql[key] = df_sql[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                        df_xml[key] = df_xml[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                    # elif column_data_types[key]== 'nvarchar' or column_data_types[key]== 'int' or column_data_types[key]== 'int64' or column_data_types[key]== 'bigint'  :
                    elif column_data_types[key] in ['nvarchar', 'int', 'int64', 'bigint']:
                    

                    
                        df_xml[key] = df_xml[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                        df_sql[key] = df_sql[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)

                        
            #null-values and str type conversion
            df_xml = df_xml.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
            df_sql = df_sql.replace(['nan', 'None', 'Null', 'NaN', ''], 'Null')
            df_sql = df_sql.fillna(value='Null')
            df_xml = df_xml.fillna(value='Null')
            df_sql_selected = df_sql[columns_selected]
                #df_csv_selected = df_csv[csv_columns]
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



            #  Total Count
            count_unmatched_xml = len(unmatched_rows_sql)
            count_unmatched_sql = len(unmatched_rows_xml)
            total_rows_xml = len(df_xml)
            total_rows_sql = len(df_sql)


            html_report = '<div class="report-container">'
            # html_report += '<h1 class="report-title">Comparison Results</h1>'

            #  Left column
                    #  Left column
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p>Source Count: {}</p>'.format(total_rows_xml)
            html_report += '<p>Target Count: {}</p>'.format(total_rows_sql)
            html_report += '</div>'

            # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_xml)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_sql)
            html_report += '</div>'
            html_report += '</div>'
    

            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source file completely matches the data in the  Target table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source file does not match the data in the Target table</div>'


            
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

           
            html_report += '<div class="container">'
            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_xml_html
            html_report += '</div>'

            
            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_sql_html
            html_report += '</div>'
            html_report += '</div>'

            context = {
                'data': {
                    'comparison_result': comparison,  # Replace comparison_result with the actual comparison result variable
                },
                'report': html_report,
            }

           


    ###############-----------------SSMS VS JSON--------------------###############################
        elif (file_input== "json"):
  
            csv_file_path = "C:\\TNDV\\Single_Table\\Folder_Path_SingleTable.csv"
            df_csv = pd.read_csv(csv_file_path)
            
            # Loop through each row in the CSV
            for index, row in df_csv.iterrows():
                json_path1 = row['json_path'].strip('"')  # Assuming 'json_path' is the column containing the JSON path
            json_file = json_path1 + "\\" + file_name + ".json"  # Modify 'file_name' as needed
          
            # Read data from JSON and SQL Server
            data_dict = pd.read_json(json_file)
            df_json = pd.DataFrame(data_dict)

            query = f"SELECT * FROM {table}"
            df_sql = pd.read_sql(query, conn)

            cursor = conn.cursor()
            parts = table.split('.')
            if len(parts) == 2:
                schema, table = parts
            else:
                schema = 'dbo'  # Default schema if not provided

            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'")
            
            column_data_types = {row.COLUMN_NAME: row.DATA_TYPE for row in cursor.fetchall()}
            df_sql = df_sql[df_json.columns]
            columns_selected=[]
            # df_csv.columns = df_csv.columns.str.upper()
            # for key in column_data_types_dict:
            #     for key in df_csv.columns:
            #         columns_selected.append(key)
            # df_csv.columns = df_csv.columns.str.upper()
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

                            df_sql[key] = df_sql[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')
                            df_json[key] = df_json[key].astype(str).apply(lambda x: '1' if x == 'True' else '0')

                        elif column_data_types[key]== 'nvarchar':                     
                            # if df_json[key].dtype == 'nvarchar' or df_json[key].dtype == 'int64' :
                            df_json[key] = df_json[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)
                    


            df_json.replace(['', np.nan], 'NULL', inplace=True)
            # Replace empty cells with None in the SQL Server dataframe
            df_sql.replace(['', np.nan], 'NULL', inplace=True)
            df_sql.replace([''], 'NULL')

            df_sql_selected = df_sql[columns_selected]
                #df_csv_selected = df_csv[csv_columns]
            df_json_selected = df_json[columns_selected]
            df_json_selected =df_json_selected.astype(str)
            df_sql_selected = df_sql_selected.astype(str)

            # comparison = df_sql.equals(df_json_selected)
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


            #  Total Count
            count_unmatched_json = len(unmatched_rows_sql)
            count_unmatched_sql = len(unmatched_rows_json)
            total_rows_json = len(df_json)
            total_rows_sql = len(df_sql)


            html_report = '<div class="report-container">'
            # html_report += '<h1 class="report-title">Comparison Results</h1>'

            #  Left column
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p>Source Count: {}</p>'.format(total_rows_json)
            html_report += '<p>Target Count: {}</p>'.format(total_rows_sql)
            html_report += '</div>'
            
            # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_json)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_sql)
            html_report += '</div>'
            html_report += '</div>'


            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source file completely matches the data in the  Target table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source file does not match the data in the Target table</div>'

       
            
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
                                    unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)



            html_report += '<div class="container">'

            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_json_html
            html_report += '</div>'

            
            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_sql_html
            html_report += '</div>'
            html_report += '</div>'

            context = {
                'data': {
                    'comparison_result': comparison,  # Replace comparison_result with the actual comparison result variable
                },
                'report': html_report,
            }

    ###############-----------------SSMS VS FlatFile--------------------###############################


        elif (file_input == "FlatFile"):



            csv_file_path = "C:\\TNDV\\Single_Table\\Folder_Path_SingleTable.csv"
            df_csv = pd.read_csv(csv_file_path)
            
            # Loop through each row in the CSV
            for index, row in df_csv.iterrows():
                #json_path1 = row['json_path'].strip('"')  # Assuming 'json_path' is the column containing the JSON path
                flat_path = row['flat_path'].strip('"')    # Assuming 'xml_path' is the column containing the XML path
                
            # Use json_path in your code to construct the JSON file path
            flat_file = flat_path + "\\" + file_name + ".txt" # Modify 'file_name' as needed

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
                
            separator = determine_delimiter(flat_file)


            query = f"SELECT * FROM {table}"
            df_sql = pd.read_sql(query, conn)

            cursor = conn.cursor()
            parts = table.split('.')
            if len(parts) == 2:
                schema, table = parts
            else:
                schema = 'dbo'  # Default schema if not provided

            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'")

            column_data_types = {row.COLUMN_NAME: row.DATA_TYPE for row in cursor.fetchall()}


            if separator:
                df_csv = pd.read_csv(flat_file, delimiter=separator)
            else:
                raise ValueError("Could not determine the delimiter of the flat file")


            # df_sql = df_sql[df_csv.columns]
            columns_selected=[]

            # columns_selected = df_csv.columns

            for column_name in df_csv.columns:
                    if column_name in column_data_types:
                        columns_selected.append(column_name)


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

                    elif column_data_types[key]== 'nvarchar':
                    

                        df_csv[key] = df_csv[key].apply(lambda x: str(x).replace('.0', '') if pd.notnull(x) else x)






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




            count_unmatched_csv = len(unmatched_rows_csv)
            count_unmatched_sql = len(unmatched_rows_sql)
            total_rows_csv = len(df_csv_selected)
            total_rows_sql = len(df_sql_selected)

            html_report = '<div class="report-container">'
                
 
            #  Left column
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p>Source Count: {}</p>'.format(total_rows_csv)
            html_report += '<p>Target Count: {}</p>'.format(total_rows_sql)
            html_report += '</div>'

            # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_csv)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_sql)
            html_report += '</div>'
            html_report += '</div>'
        

            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source file completely matches the data in the  Target table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source file does not match the data in the Target table</div>'
            

            
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


        

            html_report += '<div class="container">'

            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_csv_html
            html_report += '</div>'

            
            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_sql_html
            html_report += '</div>'
            html_report += '</div>'
            

            context = {
                'data': {
                    'comparison_result': comparison,  # Replace comparison_result with the actual comparison result variable
                },
                'report': html_report,
            }

    ###############-----------------SSMS VS SNOWFLAKE--------------------###############################

        elif (file_input == "Snowflake"):



            data = pd.read_excel('C:\TNDV\Single_Table\Single_Table_Connection.xlsx',sheet_name='Snowflake_Connection')
            connection_dict = {}
            for index, row in data.iterrows():
                connection_dict[row['Connection_Fields'].strip()] = row['Connection_Details']

            for key in connection_dict:
         
                if key == "account":
                    account = connection_dict[key]
                 
                elif key =="warehouse":
                    warehouse = connection_dict[key]
                elif key =="database":
                    database = connection_dict[key]
                elif key =="schema":
                    schema = connection_dict[key]
                elif key =="username":
                    username = connection_dict[key]
    
            password_sw = source_password
       
            conn_snowflake = snowflake.connector.connect(
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
                user=username,
                password=password_sw
            )

            def get_primary_keys_snowflake_source(connection, schema1, table_name):
                cursor = connection.cursor()
                query = f"""
                    SHOW PRIMARY KEYS IN TABLE {schema1}.{table_name}
                """
                cursor.execute(query)
                primary_keys = [row[4] for row in cursor.fetchall()]
                
                return primary_keys
            
            
            # table_snowflake = source_table
            table_snowflake = file_name
            table_sql = table
           

            primary_keys_table_snowflake_source = get_primary_keys_snowflake_source(conn_snowflake, schema,table_snowflake )
            # Fetch data from Snowflake table
            query_snowflake = f"SELECT * FROM {table_snowflake}"
            df_snowflake = pd.read_sql(query_snowflake, conn_snowflake)

            # Fetch data from SQL Server table
            query_sql = f"SELECT * FROM {table_sql}"
            df_sql = pd.read_sql(query_sql, conn)
            df_sql.columns = df_sql.columns.str.upper()

            # Snowflake column data types
            def get_column_data_types_snowflake():
                column_data_types_snowflake = {}
                query_snowflake = f"SHOW COLUMNS IN TABLE {table_snowflake}"
                cursor_snowflake = conn_snowflake.cursor()
                cursor_snowflake.execute(query_snowflake)
                for row in cursor_snowflake:
                    column_name = row[2]
                    data_type = row[3]
                    dict_data = json.loads(data_type)
                    for data in dict_data:
                        data = dict_data['type']
                    column_data_types_snowflake[column_name] = data
                return column_data_types_snowflake

            column_data_types_snowflake_dict = get_column_data_types_snowflake()

            # SQL Server column data types
            def get_column_data_types_sql():
                column_data_types_sql = {}
                query_sql = f"SELECT * FROM {table_sql}"
                cursor_sql = conn.cursor()
                data = cursor_sql.execute(query_sql)
                for column in data.description:
                    column_name = column[0]
                    data_type = str(column[1])
                    column_data_types_sql[column_name] = data_type
                return column_data_types_sql

            column_data_types_sql_dict = get_column_data_types_sql()

            # Select common columns between Snowflake and SQL Server tables
            columns_selected = [col for col in df_sql.columns if col in df_snowflake.columns]



            # Convert Snowflake table columns to appropriate data types
            for key in column_data_types_snowflake_dict:
                if key in columns_selected:
                    if column_data_types_snowflake_dict[key] == 'TIMESTAMP_NTZ':
                        df_snowflake[key] = pd.to_datetime(df_snowflake[key], format='%Y-%m-%d %H:%M')
                    elif column_data_types_snowflake_dict[key] == 'BOOLEAN':
                        df_snowflake[key] = df_snowflake[key].replace({'True': True, 'False': False}).astype(bool)

                    elif column_data_types_snowflake_dict[key] =="DATE":
                        df_snowflake[key] = pd.to_datetime(df_snowflake[key])
                        df_snowflake[key] = df_snowflake[key].dt.strftime('%Y-%m-%d')


                      
                        # df_snowflake[key] = df_snowflake[key].replace({'True': 1, 'False': 0}).astype(int)

            # Convert SQL Server table columns to appropriate data types
            for key in column_data_types_sql_dict:
                if key in columns_selected:
                    if column_data_types_sql_dict[key] == "<class 'datetime.datetime'>":
                        df_sql[key] = pd.to_datetime(df_sql[key], format='%Y-%m-%d %H:%M')
                    elif column_data_types_sql_dict[key] == "<class 'bool'>":
                        df_sql[key] = df_sql[key].replace({'True': True, 'False': False}).astype(bool)
                        # df_sql[key] = df_sql[key].replace({0: False, 1: True}).astype(bool)

                    elif column_data_types_sql_dict[key] == "<class 'date'>":
                        df_sql[key] = pd.to_datetime(df_sql[key])
                        df_sql[key] = df_sql[key].dt.strftime('%Y-%m-%d')




            replacement_value = 'None'
            df_snowflake.replace(['', np.nan], replacement_value, inplace=True)
            df_sql.replace(['', np.nan], replacement_value, inplace=True)


     
            df_snowflake = df_snowflake.astype(str)
            df_sql = df_sql.astype(str)
            comparison = df_snowflake[columns_selected].equals(df_sql[columns_selected])


            unmatched_rows_snowflake = df_snowflake[~df_snowflake.isin(df_sql)].dropna(how='all')
            for col in primary_keys_table_snowflake_source:
                unmatched_rows_snowflake[col] = df_snowflake[col]
            unmatched_rows_snowflake.index = unmatched_rows_snowflake.index + 1  
            unmatched_rows_snowflake = unmatched_rows_snowflake[~unmatched_rows_snowflake.isin(primary_keys_table_snowflake_source)].dropna(how='all')


            primary_keys_table_sql = [col.upper() for col in primary_keys_table_sql]
            unmatched_rows_sql = df_sql[~df_sql.isin(df_snowflake)].dropna(how='all')
            for col in primary_keys_table_sql:
                unmatched_rows_sql[col] = df_sql[col]
            
            unmatched_rows_sql.index = unmatched_rows_sql.index + 1  
            unmatched_rows_sql = unmatched_rows_sql[~unmatched_rows_sql.isin(primary_keys_table_sql)].dropna(how='all')

            #  Total Count
            count_unmatched_snowflake = len(unmatched_rows_snowflake)
            count_unmatched_sql = len(unmatched_rows_sql)
            total_rows_snowflake = len(df_snowflake)
            total_rows_sql = len(df_sql)

            context = {}
        
            html_report = '<div class="report-container">'
            # html_report += '<h1 class="report-title">Comparison Results</h1>'

            #  Left column
            html_report += '<div style="display: flex;">'
            html_report += '<div class="left-column">'
            html_report += '<h3>Table Count:</h3>'
            html_report += '<p>Source Count: {}</p>'.format(total_rows_snowflake)
            html_report += '<p>Target Count: {}</p>'.format(total_rows_sql)
            html_report += '</div>'

            # Right column
            html_report += '<div style="flex: 1;">'
            html_report += '<h4>Unmatched Data Count:</h4>'
            html_report += '<p>Unmatched Source Count: {}</p>'.format(count_unmatched_snowflake)
            html_report += '<p>Unmatched Target Count: {}</p>'.format(count_unmatched_sql)
            html_report += '</div>'
            html_report += '</div>'


            # Comparison result
            if comparison:
                html_report += '<div class="comparison-result true" >Comparison Results: The data in the Source file completely matches the data in the  Target table</div>'
            else:
                html_report += '<div class="comparison-result false" >Comparison Results: The data in the Source file does not match the data in the Target table</div>'

            
            # Set background color for unmatched rows to red and remaining cells to green
            unmatched_rows_snowflake_html = unmatched_rows_snowflake.to_html(na_rep='-', escape=False)
            unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
            unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace('<tr>', '<tr style="text-align: center">')

            for index, row in unmatched_rows_snowflake.iterrows():
                for col, value in row.items():
                    
                    if col in primary_keys_table_snowflake_source:
                                unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                    elif ( value != '-' and col not in primary_keys_table_snowflake_source):
                                unmatched_rows_snowflake_html = unmatched_rows_snowflake_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)


            unmatched_rows_sql_html = unmatched_rows_sql.to_html(na_rep='-', escape=False)
            unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<td>-</td>', '<td style="background-color: lightgreen">-</td>')
            unmatched_rows_sql_html = unmatched_rows_sql_html.replace('<tr>', '<tr style="text-align: center">')

            for index, row in unmatched_rows_sql.iterrows():
                for col, value in row.items():
                    
                    if col in primary_keys_table_sql:
                                unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color:lightgreen ">{value}</td>',1)
                    elif ( value != '-' and col not in primary_keys_table_sql):
                                unmatched_rows_sql_html = unmatched_rows_sql_html.replace(f'<td>{str(value).strip()}</td>',f'<td style="background-color: red">{value}</td>',1)

           

            
            html_report += '<div class="container">'

            html_report += '<div class="left-table">'
            html_report += '<h4>Unmatched data from Source:</h4>'
            html_report += unmatched_rows_snowflake_html
            html_report += '</div>'

            
            html_report += '<div class="right-table">'
            html_report += '<h4>Unmatched data from Target:</h4>'
            html_report += unmatched_rows_sql_html
            html_report += '</div>'
            html_report += '</div>'

            

            context = {
                'data': {
                    'comparison_result': comparison,  # Replace comparison_result with the actual comparison result variable
                },
                'report': html_report,
            }

    

    return Response(json.dumps(context), status=200, mimetype='application/json')


            

