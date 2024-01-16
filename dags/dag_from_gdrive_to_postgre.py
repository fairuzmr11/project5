from datetime import datetime, timedelta
import subprocess

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import variable

from airflow.hooks.postgres_hook import PostgresHook

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly",
          "https://www.googleapis.com/auth/drive.file",
          "https://www.googleapis.com/auth/drive.readonly",
          "https://www.googleapis.com/auth/drive"]

def install_package(package_list):
    for package in package_list:
        try:
            subprocess.check_call(['pip3', 'install', package])
            print(f"successfully installed {package}")
        except subprocess.CalledProcessError:
            print(f"Error installing {package}")

def authenticate_():
    import json
    from google.oauth2.credentials import Credentials

    packages_to_install = ['google-api-python-client','google-auth-httplib2','google-auth',
                           'google-auth-oauthlib']
    install_package(packages_to_install)
    
    creds = None
    # get token from airflow variable
    token_path = variable.key(key="dag_from_gdrive_to_postgre.json")
    json_token = json.loads(token_path)

    if json_token:
        print("Credentials from token", json_token['token'])
        creds = Credentials(
            token = json_token['token'],
            refresh_token= json_token['refresh_token'],
            token_uri= json_token['token_uri'],
            client_id= json_token['client_id'],
            client_secret= json_token['client_secret'],
            scopes= json_token['scopes']
        )
    print(creds)

    return creds

def get_folder_id(service,folder_name):
    query = f"sharedWithMe=true and name='{folder_name}'"
    results = (
        service.files()
        .list(q=query, pageSize=10, fields="nextPageToken, files(id, name)")
        .execute()
    )
    items = results.get("files", [])
    return items

def download_file(service, file_id, file_name):
    import io
    from googleapiclient.http import MediaIoBaseDownload

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False

    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}.")
    
    return fh.getvalue()

    #     while done is False:
    #       status, done = downloader.next_chunk()
    #       print(f"Download {int(status.progress() * 100)}.")

    #   except HttpError as error:
    #     print(f"An error occurred: {error}")
    #     file = None

    #   return file.getvalue()

def check_file_exists(ti):
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError

    try:
        creds = authenticate_()
        service = build("drive", "v3", credentials=creds, cache_discovery=False)

        dataset_folder_name = 'dataset'
        dataset_folders = get_folder_id(service,dataset_folder_name)

        if dataset_folders == []:
            print(f"The {dataset_folder_name} files does not exist in 'Shared with me' in folder 'dataset'.")
            return "end"
        
        # get id of 'dataset folder
        dataset_folder_id = dataset_folders[0]['id']
        print(f" The {dataset_folder_name} foleder exists in 'Shared with me'.")

        # get files in 'dataset' folder
        dataset_files = (
            service.files()
            .list(q=f"'{dataset_folder_id}' in parents", pageSize=10, fileds="nextPageToken, files(id, name)")
            .execute()
        ).get("files", [])

        print("dataset_files :", dataset_files)

        # download files in 'dataset' folder
        if dataset_files != []:
            list_name = []
            for file in dataset_files:
                file_name = file['name']
                file_id = file['id']
                download_file(service, file_id, file_name)
                list_name.append(file_name)
            
            # push list_name to xcom
                ti.xcom_push(key='dataset_files', value=list_name)
                return "process_csv"
        else:
            print("No files found in 'dataset' folder.")
            return "end"
    except HttpError as e:
        print(f"an error occurred: {e}")

def compose_values(row):
    return f"{row['IP']}_{row['UserAgent']}_{row['Country']}_{row['Languages']},_{row['Interests']}"

def process_csv(ti):
    import csv

    # pull xcom
    dataset_files = ti.xcom_pull(key='dataset_files', task_ids='check_file_if_exist')

    # process each file
    for file in dataset_files:
        print("file: ", file)
        input_file = file
        output_file = file.split(".")[0] + 'processed_csv'

        with open(input_file, 'r', newline='') as csvfile:

            # open the output CSV file for writing
            list_processed_name = []

            with open(output_file, 'w', newline='') as output_csvfile:
                # create csv reader and writer objects
                reader = csv.DictReader(csvfile)
                filednames = reader.fieldnames + ['id']
                writer = csv.DictWriter(output_csvfile, filednames=filednames)

                # Write the header to the output CSV file
                writer.writeheader()

                #  process each row
                for row in reader:
                    # compose the new filed value
                    new_field_value = compose_values(row)

                    #  add the new field to the row
                    row['id'] = new_field_value

                    # write the modified row to the output csv file
                    writer.writerow(row)

                list_processed_name.append(output_file)
            ti.xcom_push(key='list_processed_name', value=list_processed_name)

def create_ddl():
    import os

    #  install psycopg2
    pacakges_to_install = ['psycopg2-binary']
    install_package(pacakges_to_install)

    import psycopg2

    ddl = """
    CREATE TABLE IF NOT EXISTS public.network_data_test (
    id TEXT PRIMARY KEY,
    IP VARCHAR(15),
    user_agent TEXT,
    country VARCHAR(255),
    languages VARCHAR(255),
    interests TEXT,
    created at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    connection = None
    print ("Connecting to the PostgreSQ: database... os.environ.get('POSTGRES_USER')", os.environ.get('POSTGRES_USER'))
    try:
        connection = psycopg2.connect(
            host = '172.18.0.2',
            port = 5432,
            dbname = 'airflow',
            user = 'airflow',
            paswords = 'airflow'
        )

        with connection.curser() as cursor:
            cursor.execute(ddl)
        connection.commit()

        connection.close()

        print("Table creation successful.")
    except Exception as e:
        print(f"Error: {e}")
        raise e
    
def upsert_data(ti):
    import pandas as pd
    import psygopg2
    from sqlalchemy import create_engine

    # PostgreSQL connection parameters
    db_params = {
        'dbname' : 'airflow',
        'user' : 'airflow',
        'password': 'airflow',
        'host' : '172.18.0.2',
        'port' : '5432'
    }
    # CSV file path
    # pull xcom
    list_processed_name = ti.xcom_pull(key="list_processed_name", task_ids="process_csv")

    for file in list_processed_name:

        df = pd.read_csv(file)
        # Connect to Postgres
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
        df.to_sql('network_data', engine, if_exists='replace', index=False)


with DAG(
    dag_id='dag_from_gdrive_to_postgres',
    start_date=datetime(2023, 1, 1),
    schedule_interval='00 23 * * *',
    catchup=False,
    default_args={
        'retries' : 1,
        'retry_delay': timedelta(minutes=3)
    }
) as dag:
    
    start = EmptyOperator(task_id='start')


    check_file_exists = PythonOperator(
        task_id='check_file_exists',
        python_callable=check_file_exists,
        execution_timeout=timedelta(seconds=30),
        retries=3,
        retry_delay=timedelta(seconds=30)
    )

    process_csv = PythonOperator(
        task_id = 'process_csv',
        python_callable = process_csv,
        execution_timeout=timedelta(seconds=30),
        retries = 2,
        retry_delay=timedelta(seconds=30) 
    )

    create_ddl = PythonOperator(
        task_id = 'create_ddl',
        python_callable=create_ddl,
        execution_timeout=timedelta(seconds=30),
        retries = 2,
        retry_delay=timedelta(seconds=30)        
    )

    upsert_data = PythonOperator(
        task_id = 'upsert_data',
        python_callable=upsert_data,
        execution_timeout=timedelta(seconds=30),
        retries = 2,
        retry_delay=timedelta(seconds=30)          
    )

    end = EmptyOperator(task_id='end')    


start >> check_file_exists >> [process_csv, end]
process_csv >> create_ddl >> upsert_data >> end