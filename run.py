import psycopg2
from time import sleep
from datetime import datetime
from data_utils.aws.aws_helpers import read_from_s3_to_pandas
from data_utils.aws.aws_helpers import explode_column
from data_utils.aws.aws_helpers import connect_to_aws_service
from data_utils.aws.aws_helpers import stream_dataframe_to_postgres_table
import pandas as pd
import json
import psycopg2
import configparser
import logging
import os

#setting logginf configuration
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


# Parsing Arguments from config.ini file
config = configparser.ConfigParser()
config.read('config.ini')

bucketname=config['bucket']['bucketname']
itemnames=config['bucket']['itemnames'].split(',')
aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']

#paths to sql files
DML_path='./sql/DML/'
DDL_path='./sql/DDL/'


try:
    #connecting to Database and Autocommit mode
    connection=psycopg2.connect(user='user',
                            password='password',
                            host='postgres',
                            database='database')
    connection.autocommit = True
    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    logging.info('connecetion parameters: {parameters}'.format(parameters=connection.get_dsn_parameters()))

    #Creating a list of pg keywords to avoid using them in columns, tables....
    cursor.execute('select * from pg_get_keywords()')
    record = cursor.fetchall()
    psqlkeywords=[i[0] for i in record]
    psqlkeywords_upper=[i[0].upper() for i in record]
    logging.info('Getting a list of psql_keywords, in order to avoid keyworded columns names ')

    #getting S3 client
    s3=connect_to_aws_service(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_service='s3'
    )


    df=pd.DataFrame()
    logging.info('Creating empty DataFrame to append data to it') 
    
    #looping over files in s3 and appending them to dataframe
    for itemname in itemnames:
        df_stg=read_from_s3_to_pandas(s3,bucketname,itemname,date_column='TIMESTAMP')
        now=str(datetime.now())
        df_stg.insert(len(df_stg.columns),'inserted_at', now)
        logging.info('Adding column inserted_at to DataFrame') 
        df=df.append(df_stg, ignore_index=True)
        logging.info('Data appended to DataFrame ') 

    #lowers case all attributes to avoid key_problems
    df['ATTRIBUTES']=df['ATTRIBUTES'].str.lower()

    #exploiding column ATTRIBUTES
    df=explode_column(df,'ATTRIBUTES')

    df = df.replace(r'\n',' ', regex=True) 

    #column names transformation
    df=df.rename(lambda col_name: col_name.lower() , axis='columns')
    df=df.rename(lambda col_name: col_name.replace('(','_') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace(')','') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace(' ','_') , axis='columns')
    df=df.rename(lambda col_name: col_name + '_' if col_name in psqlkeywords or col_name in psqlkeywords_upper else col_name, axis='columns')

    #dropping and recreating Schemas (Just to make the file rerunable)
    sqlfile = open(DDL_path +'landing_create_schema_performance.sql', 'r')
    cursor.execute(sqlfile.read())
    logging.info('Drop landing and bip Schemas')

    # Automatic Create table Statement using DataFrame schema
    create_statement=pd.io.sql.get_schema(df,"landing_performance.stg_performance")
    create_statement=create_statement.replace('"','')
    create_statement=create_statement.replace('INTEGER','BOOLEAN')
    logging.info('Executing CREATE STATEMENT:{create_statement}'.format(create_statement=create_statement))
    cursor.execute(create_statement)

    # Insert step
    stream_dataframe_to_postgres_table(connection=connection,dataframe=df,table='landing_performance.stg_performance')

    #Create table for_insert table
    sqlfile = open(DDL_path +'landing_create_performance_for_insert.sql', 'r')
    cursor.execute(sqlfile.read())
    logging.info('Created landing_performance.performance_for_insert like landing_performance.stg_performance')

    sqlfile = open(DML_path +'landing_insert_performance_for_insert.sql', 'r')
    cursor.execute(sqlfile.read())
    logging.info('Removed possible duplicates and inserting in landing_performance.performance_for_insert')

    
    sqlfile = open(DDL_path +'bip_create_article_performance.sql', 'r')
    cursor.execute(sqlfile.read())
    logging.info('Created bip_performance.article_performance')

    sqlfile = open(DDL_path +'bip_create_user_performance.sql', 'r')
    cursor.execute(sqlfile.read())
    logging.info('Created bip_performance.user_performance')
    
    #Transformation step
    sqlfile = open(DML_path +'bip_insert_article_performance.sql', 'r')
    cursor.execute(sqlfile.read())
    logging.info('Data inserted in  bip_performance.article_performance')

    sqlfile = open(DML_path +'bip_insert_user_performance.sql', 'r')
    cursor.execute(sqlfile.read())
    logging.info('Data inserted in  bip_performance.user_performance')

except (Exception, psycopg2.Error) as error:
    logging.error('Error while connecting to PostgreSQL:{error}',error=error)

finally:
    connection.close()
    logging.info('Pipeline Finished')
    logging.info('Clossing Connection')
