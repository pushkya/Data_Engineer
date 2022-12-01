import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}") #-O is output 

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize = 100000)

    df = next(df_iter)

    df.head(n=0).to_sql(name = table_name, con = engine, if_exists='replace')

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            df.to_sql(name = table_name, con = engine, if_exists='append')
            t_end = time()
            print('Inserted Chunk..., took %3.f seconds'%(t_end-t_start))
        except StopIteration:
            print('Finished ingesting data into postgres database')
            break

if __name__ == '__main__':

#user, password, host, port, database name, table name
#URL of csv
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

    parser.add_argument('--user', help='User name for postgres')
    parser.add_argument('--password', help='Password for postgres')
    parser.add_argument('--host', help='Host for postgres')
    parser.add_argument('--port', help='Port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='Table name for writing results')
    parser.add_argument('--url', help='URL for csv file')

    args = parser.parse_args()
    main(args)



 