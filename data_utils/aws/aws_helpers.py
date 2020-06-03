import boto3
import pandas as pd
import io 
import json
from io import StringIO
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def connect_to_aws_service(aws_access_key_id,aws_secret_access_key,aws_service):

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)

    client = session.client('s3')
    
    logging.info('Returning s3 Client')

    return client


def read_from_s3_to_pandas(client,bucketname,itemname,date_column):

    logging.info('Reading into a Pandas DataFrame Item: {itemname}, from Bucket: {bucketname}'.format(bucketname=bucketname,itemname=itemname))

    obj = client.get_object(Bucket=bucketname, Key=itemname)
    
    logging.info('Column {date_column} will be parsed as date'.format(date_column=date_column))

    df = pd.read_csv(io.BytesIO(obj['Body'].read()),
                 parse_dates=[date_column],
                 date_parser=pd.to_datetime,
                 sep='\t',
                 header='infer',
                 encoding='utf-8')
    
    df['bucketname']=bucketname
    df['itemname']=itemname
    
    return df

def explode_column(df,column_to_explode):


    logging.info('Exploding {column_to_explode} column in DataFrame'.format(column_to_explode=column_to_explode))

    df[column_to_explode]=df[column_to_explode].fillna('{}')
    json_explode=df[column_to_explode].apply(json.loads).apply(pd.Series)
    json_explode=json_explode.rename(lambda col_name: 'attributes_'+ col_name, axis='columns')
    df_exploded=pd.concat([df, json_explode], axis=1)
    
    logging.info('New columns names will be prefixed with {column_to_explode}_'.format(column_to_explode=column_to_explode.lower()))

    logging.info('New Columns are {columns}'.format(columns=df_exploded.columns))

    return df_exploded


def stream_dataframe_to_postgres_table(connection,dataframe,table):
    
    sio = StringIO()
    sio.write(dataframe.to_csv(index=None, header=None, sep="\t"))
    sio.seek(0)

    logging.info('Inserting Dataframe into {table} table'.format(table=table))

    # Copy the string buffer to the database, as if it were an actual file
    with connection.cursor() as c:
        c.copy_from(sio, table, columns=dataframe.columns, sep="\t",null='')
        connection.commit()

    logging.info('Data Set Completely inserted')

