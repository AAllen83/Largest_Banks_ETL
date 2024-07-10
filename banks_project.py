'''Project Scenario:
You have been hired as a data engineer by research organization. Your boss has asked you to create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available to you as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table.

Your job is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.'''

'''Project tasks
Task 1:
Write a function log_progress() to log the progress of the code at different stages in a file code_log.txt. Use the list of log points provided to create log entries as every stage of the code.

Task 2:
Extract the tabular information from the given URL under the heading 'By market capitalization' and save it to a dataframe.
a. Inspect the webpage and identify the position and pattern of the tabular information in the HTML code
b. Write the code for a function extract() to perform the required data extraction.
c. Execute a function call to extract() to verify the output.

Task 3:
Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
a. Write the code for a function transform() to perform the said task.
b. Execute a function call to transform() and verify the output.

Task 4:
Load the transformed dataframe to an output CSV file. Write a function load_to_csv(), execute a function call and verify the output.

Task 5:
Load the transformed dataframe to an SQL database server as a table. Write a function load_to_db(), execute a function call and verify the output.

Task 6:
Run queries on the database table. Write a function load_to_db(), execute a given set of queries and verify the output.

Task 7:
Verify that the log entries have been completed at all stages by checking the contents of the file code_log.txt.'''

''' Commands needed for setup
python3.11 -m pip install requests
python3.11 -m pip install bs4
python3.11 -m pip install pandas
python3.11 -m pip install datetime
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'''

# Code for ETL operations on Country-GDP data
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
log_file = 'code_log.txt'

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text                                       #Extract the web page as text
    data = BeautifulSoup(page,'html.parser')                            #Parse the text into an HTML object
    df = pd.DataFrame(columns=["Name", "MC_USD_Billion"])                          #Create an empty pandas DataFrame named df with columns as the table_attribs
    tables = data.find_all('tbody')                                     #gets the body of all the tables in the web page
    rows = tables[0].find_all('tr')                                     #gets all the rows of the first table
    for row in rows:                                                    #Iterate over the contents of the variable rows.
        col = row.find_all('td')                                        #Extract all the td data objects in the row and save them to col.
        if len(col)!=0:                                                 #Check if the length of col is 0, that is, if there is no data in a current row. This is important since, many timesm there are merged rows that are not apparent in the web page appearance.
            data_dict = {"Name": col[1].find_all('a')[1]['title'],
                         "MC_USD_Billion": float(col[2].contents[0][:-1])}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    import csv
    # read csv
    data = pd.read_csv('exchange_rate.csv')
    # Convert the DataFrame to a Dictionary
    data_dict = data.to_dict(orient='list')
    #Adding new columns
    df['MC_GBP_Billion'] = ""
    df['MC_EUR_Billion'] = ""
    df['MC_INR_Billion'] = ""
    for row in df['MC_USD_Billion']:
        df['MC_GBP_Billion'] = [np.round(x*data_dict['Rate'][1],2) for x in df['MC_USD_Billion']] #MC_GBP_Billion conversion
        df['MC_EUR_Billion'] = [np.round(x*data_dict['Rate'][0],2) for x in df['MC_USD_Billion']] #MC_EUR_Billion conversion
        df['MC_INR_Billion'] = [np.round(x*data_dict['Rate'][2],2) for x in df['MC_USD_Billion']] #MC_INR_Billion conversion
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_queries(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    
''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
log_progress('Preliminaries complete. Initiating ETL process')

log_progress('Data extraction complete. Initiating Transformation process')

df = (extract(url, table_attribs))

log_progress('Data transformation complete. Initiating loading process')

(transform(df, csv_path))

load_to_csv(df, './Largest_banks_data.csv')

sql_connection = sqlite3.connect('Banks.db')

load_to_db(df, sql_connection, 'Largest_banks')

log_progress('Data loaded to Database as table. Running the query')

'''Queries:
            SELECT * FROM Largest_banks
            SELECT AVG(MC_GBP_Billion) FROM Largest_banks
            SELECT Name from Largest_banks LIMIT 5'''
query_statement = "SELECT Name from Largest_banks LIMIT 5"
run_queries(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()