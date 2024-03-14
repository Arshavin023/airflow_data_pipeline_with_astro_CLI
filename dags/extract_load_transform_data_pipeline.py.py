from datetime import datetime, timedelta
import os
from pathlib import Path
from io import StringIO
import pandas as pd
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from botocore.client import Config
from botocore import UNSIGNED
import boto3
import numpy as np
from sqlalchemy import create_engine
from airflow.utils.dates import days_ago

import logging
import traceback

# 1. Logging Configuration
log_file_path = 'error_log.txt'
logging.basicConfig(filename=log_file_path, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# def convert_to_str(value):
#     """
#     Convert the value to string, handling specific types to avoid errors.
#     """
#     if isinstance(value, (int, np.int64, np.int32, np.int16, np.int8)):
#         return str(value)
#     elif isinstance(value, (float, np.float64, np.float32)):
#         return str(value)
#     elif isinstance(value, (datetime, pd.Timestamp)):
#         return value.strftime('%Y-%m-%d %H:%M:%S')
#     elif isinstance(value, str):
#         return value
#     else:
#         return str(value)

    

# 2. DAG Definition
@dag(
    'extract_transform_load_updated',
    default_args={
        'owner': 'uchejudennodim@gmail.com',
        'depends_on_past': False,
        'start_date': days_ago(0),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Download orders, reviews and shipment data from S3 to file directory, load into Postgres, transform and upload to S3',
    schedule_interval='0 1,23 * * *',
    concurrency=1,
)
def daily_extraction_dag():
    # Replace these values with your PostgreSQL database details
    db_user = Variable.get('D2B_POSTGRES_USERNAME')
    db_password = Variable.get('D2B_POSTGRES_PASSWORD')
    db_host = Variable.get('D2B_POSTGRES_HOST')
    db_port = Variable.get('D2B_POSTGRES_PORT')
    db_name = Variable.get('D2B_POSTGRES_DATABASE')

    # Construct the connection string
    conn_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

    # Create an SQLAlchemy engine using the connection string
    engine = create_engine(conn_string)

    # 3. Task Function Signature
    @task(task_id="download_files_task")
    def download_files_from_s3(bucket_name: str) -> str:
        try:
            s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
            local_directory = "/usr/local/airflow/data"
            os.makedirs(local_directory, exist_ok=True)
            current_date = datetime.now().strftime('%Y-%m-%d')
            current_date_folder = f'raw_files_{current_date}'
            folder = os.path.join(local_directory, current_date_folder)
            if not os.path.exists(folder):
                os.makedirs(folder)
                for file_name in ["orders.csv", "reviews.csv", "shipment_deliveries.csv"]:
                    s3.download_file(bucket_name, f"orders_data/{file_name}", os.path.join(folder, file_name))
            else:
                for file_name in ["orders.csv", "reviews.csv", "shipment_deliveries.csv"]:
                    s3.download_file(bucket_name, f"orders_data/{file_name}", os.path.join(folder, file_name))
                    
            logging.info('Files successfully downloaded')
            return folder

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")

    @task(task_id="read_csv_task")
    def read_csv_files_into_dictionary(raw_folder_path: str, **kwargs) -> dict:
        try:
            files = os.listdir(raw_folder_path)
            csv_files = [file for file in files if file.endswith('.csv')]
            dataframes_dict = {}
            for csv_file in csv_files:
                file_path = os.path.join(raw_folder_path, csv_file)
                df = pd.read_csv(file_path, index_col=None)
                key = csv_file.split('.')[0]
                dataframes_dict[key] = df
            logging.info('Successfully read CSV files from filepath and saved into dictionary')
            return dataframes_dict

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")

    @task(task_id="connect_to_postgres")
    def connect_to_postgres_and_load_data(dataframe_dict):
        try:
            for table_name, df in dataframe_dict.items():
                if table_name != 'reviews':
                    primary_key_column = list(df.columns)[0]
                    conn = engine.connect()
                    # Check for existing records before insert
                    max_num_query = f'''
                    SELECT CASE WHEN MAX(CAST({primary_key_column} AS INT)) IS NULL THEN 0
                    ELSE MAX(CAST({primary_key_column} AS INT)) END AS max_num
                    FROM azeeolow8759_staging.{table_name}'''

                    existing_records = conn.execute(max_num_query)
                    max_id = existing_records.fetchone()['max_num']

                    # Filter income dataframe and check if it's empty
                    filtered_df = df.query(f'{primary_key_column} > {max_id}')
                    if not filtered_df.empty:
                        filtered_df.to_sql(f'azeeolow8759_staging.{table_name}', engine, if_exists='append', index=False)
                        logging.info(f'Data successfully inserted into azeeolow8759_staging.{table_name} on PostgreSQL')

                    else:
                        logging.info("No new data and nothing loaded into SQLite database")

                else:
                    df.to_sql(f'azeeolow8759_staging.{table_name}', engine, if_exists='append', index=False)
                    logging.info(f'stage completed.{table_name} on PostgreSQL')

            return "all csv files successfully loaded into staging in Postgres"

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")

    @task(task_id="transformation_in_postgres")
    def perform_transformation() -> str:
        sql_file_path = Path("/usr/local/airflow/sql/transform.sql")
        
        try:
            with open(sql_file_path, 'r') as file:
                sql_query = file.read()
                conn = engine.connect()
                conn.execute(sql_query)
                logging.info('Successfully performed transformation in PostgreSQL')

            return 'Transformation successfully completed'

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")

    @task(task_id="postgres_to_S3")
    def upload_transformed_data_s3(bucket_name: str):
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        query_list = ['SELECT * FROM azeeolow8759_analytics.agg_public_holiday',
                      'SELECT * FROM azeeolow8759_analytics.agg_shipments',
                      'SELECT * FROM azeeolow8759_analytics.best_performing_product']
        try:
            conn = engine.connect()
            for query in query_list:
                result = conn.execute(query)
                column_names = result.keys()
                transformed_data = result.fetchall()
                df = pd.DataFrame(transformed_data, columns=column_names)
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                path_in_s3 = '/analytics_export/'
                table_name = query.split('.')[1]
                key = f"{path_in_s3}{table_name}.csv"
                s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=key)
                logging.info(f'Successfully uploaded {table_name} into S3 bucket')

            logging.info('Successfully uploaded all transformed files into S3 bucket')
            return 'Successfully uploaded transformed data into S3'

        except Exception as e:
            raise RuntimeError(f"Exception occurred: {str(e)}\n{traceback.format_exc()}")
    
    # Postgres database credentials
    current_date = datetime.now().strftime('%Y-%m-%d')
    current_date_folder = Path('raw_files'+f'_{current_date}')
    bucket_name = Variable.get('BUCKET_NAME')
    download_task = download_files_from_s3(bucket_name)
    dataframe_dict = read_csv_files_into_dictionary(download_task)
    loading_data = connect_to_postgres_and_load_data(dataframe_dict)
    transformation = perform_transformation()
    final_stage = upload_transformed_data_s3(bucket_name)

    # Set dependencies
    download_task >> dataframe_dict >> loading_data >> transformation >> final_stage
    #>> load_dataframes_into_postgres >> perform_transformation_in_postgres >> download_and_upload_transformed_data_S3
     
daily_extraction_dag()



        
