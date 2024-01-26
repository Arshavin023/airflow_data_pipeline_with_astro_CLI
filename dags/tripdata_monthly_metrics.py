from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
from airflow.decorators import dag, task
from airflow.models import Variable
import sqlite3
from airflow.utils.dates import days_ago
import logging
import traceback
from clickhouse_driver import Client

# 1. Logging Configuration
log_file_path = 'error_log.txt'
logging.basicConfig(filename=log_file_path, level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 2. DAG Definition
@dag(
    'tripdata_monthly_metrics',
    default_args={
        'owner': 'uchejudennodim@gmail.com',
        'depends_on_past': False,
        'start_date': days_ago(0),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Extracts monthly metrics from tripdata table in MySQL, calculates aggregations, and loads into SQLite.',
    schedule_interval='@monthly',
    concurrency=1,
)

def monthly_extraction_dag():

    @task(task_id="connect_to_tripdata_database_and_extract_metrics")
    def connect_to_tripdata_database():
        """
        Task to connect to TripData Database and extract metrics from the tripdata table.

        - Connects to Clickhouse using credentials stored as Airflow variables.
        - Executes a SQL query to extract metrics related to pickup dates between 2014-01-01 and 2016-12-31.
        - Calculates various aggregated metrics grouped by year-month.
        - Returns the results as a Pandas DataFrame.
        """
        clickhouse_host = Variable.get('CLICKHOUSE_HOSTNAME')
        clickhouse_user = Variable.get('CLICKHOUSE_USERNAME')
        clickhouse_password = Variable.get('CLICKHOUSE_PASSWORD')
        clickhouse_database = Variable.get('CLICKHOUSE_DATABASE')

        try:
            client = Client(host=clickhouse_host, 
                            # port=clickhouse_port, 
                            user=clickhouse_user, 
                            password=clickhouse_password, 
                            database=clickhouse_database,
                            secure=True)
            print('successfully connected to clickhouse')

            # Your ClickHouse query or operation
            query = '''with 2014_01_01_to_2016_01_01 as 
                        (select pickup_date, pickup_datetime, dropoff_datetime, fare_amount 
                        from tripdata 
                        where pickup_date between '2014-01-01' and '2016-12-31')

                        select DATE_FORMAT(pickup_date, '%Y-%m') AS year_month,
                        round(avg(case when DAYOFWEEK(pickup_date) = 7 THEN 1 ELSE 0 END),2) sat_mean_trip_count,
                        round(avg(case when DAYOFWEEK(pickup_date) = 7 THEN fare_amount else 0 END),2) sat_mean_fare_per_trip,
                        round(avg(case when DAYOFWEEK(pickup_date) = 7 THEN TIMESTAMPDIFF(MINUTE, pickup_datetime, dropoff_datetime) else 0 END),2) sat_mean_duration_per_trip_in_minutes,
                        round(avg(case when DAYOFWEEK(pickup_date) = 1 THEN 1 ELSE 0 END),2) sun_mean_trip_count,
                        round(avg(case when DAYOFWEEK(pickup_date) = 1 THEN fare_amount else 0 END),2) sun_mean_fare_per_trip,
                        round(avg(case when DAYOFWEEK(pickup_date) = 1 THEN TIMESTAMPDIFF(MINUTE, pickup_datetime, dropoff_datetime) else 0 END),2) sun_mean_duration_per_trip_in_minutes
                        from 2014_01_01_to_2016_01_01
                        group by 1
                        order by DATE_FORMAT(pickup_date, '%Y-%m')'''
            
            result = client.execute(query)
            
            # Do something with the result, or perform other ClickHouse operations
            columns = ['year_month','sat_mean_trip_count','sat_mean_fare_per_trip','sat_mean_duration_per_trip_in_minutes',
                    'sun_mean_trip_count','sun_mean_fare_per_trip','sun_mean_duration_per_trip_in_minutes']
            df = pd.DataFrame(result, columns=columns)
            client.disconnect()
            # print(df)
            return df

        except Exception as err:
            logging.info(f"Error: {err}")

    @task(task_id="connect_to_sqlite")
    def write_metrics_to_sqlite(dataframe: DataFrame, **kwargs):
        """
        Task to connect to SQLite and write metrics.

        - Connects to SQLite using a connection string stored as an Airflow variable.
        - Writes metrics extracted from MySQL into an SQLite table named 'metrics_table'.
        - Logs information about the process, including connections and successful completion.
        - Handles exceptions by logging errors and raising a RuntimeError with detailed error information.
        """
        try:
            # Connect to the SQLite database
            sqlite_connection_string = Variable.get("sqlite_connection_string")
            logging.info(f"Connecting to SQLite database using connection string: {sqlite_connection_string}")
            sqlite_conn = sqlite3.connect(sqlite_connection_string)
            

            # Write metrics to SQLite database
            logging.info("Writing metrics to SQLite database.")
            dataframe.to_sql('metrics_table', con=sqlite_conn, if_exists='replace', index=False)

            # Close the database connection
            sqlite_conn.close()
            logging.info("SQLite connection closed. Task successfully completed.")

            return 'successfully completed'

        except Exception as e:
            error_message = f"Exception occurred: {str(e)}\n{traceback.format_exc()}"
            logging.error(error_message)
            raise RuntimeError(error_message)

        
    dataframe = connect_to_tripdata_database()
    loading_data = write_metrics_to_sqlite(dataframe)

    # Set dependencies
    dataframe >> loading_data
    #>> load_dataframes_into_postgres >> perform_transformation_in_postgres >> download_and_upload_transformed_data_S3
     
monthly_extraction_dag()
