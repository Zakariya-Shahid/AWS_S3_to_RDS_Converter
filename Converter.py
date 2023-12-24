import time

import psycopg2
import boto3
from botocore.exceptions import ClientError


def get_secret():

    # secret_name = os.getenv("RDS_SECRET_NAME")
    secret_name = "preprod/doorfeeddb"

    # region_name = os.getenv("S3_REGION_NAME")
    region_name = "eu-west-3"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return secret
    # Your code goes here.

secret = get_secret()
# print(secret)


# Configure PostgreSQL connection parameters
DB_HOST = secret.split(',')[3].split(':')[1].strip('"')
DB_PORT = secret.split(',')[4].split(':')[1].strip('"')
DB_NAME = secret.split(',')[5].split(':')[1].strip('"')
DB_NAME = DB_NAME[:-2]
DB_USER = secret.split(',')[0].split(':')[1].strip('"')
DB_PASSWORD = secret.split(',')[1].split(':')[1].strip('"')


# Establish a connection to the RDS database
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

# Execute a SQL query to retrieve data from a table
cur = conn.cursor()

def rds_to_s3():
    bucket = 'doorfeed-preprod-rds-data'
    region = 'eu-west-3'
    # bucket = os.getenv("S3_BUCKET_NAME")
    # region = os.getenv("S3_REGION_NAME")

    # schema = os.getenv("SCHEMA_NAME")
    schema = 'public'
    # getting all tables in the database excluding view
    # cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema} AND table_type = 'BASE TABLE';")
    # tables = cur.fetchall()

    # tab = os.getenv("RDS_TABLE_NAME")
    tab = 'spatial_ref_sys'
    # getting the columns of the table along with their data types
    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{tab}';")
    columns = cur.fetchall()
    print(columns)

    # creating a query string which adds to_char() function to the date columns of data type "timestamp with time zone"
    query = f"SELECT "
    for column in columns:
        if column[1] == 'timestamp with time zone':
            query += f"to_char({column[0]}, ''yyyymmdd'') as {column[0]},"
        else:
            query += f"{column[0]}, "
    query = query[:-2]

    query += f" FROM {schema}.{tab}"

    # adding the query to fetch the result in a date range using to from sql
    #query += f" WHERE {columns[0][0]} >= to_date('2021-01-01', ''yyyy-mm-dd'') AND {columns[0][0]} <= to_date('2021-01-31', ''yyyy-mm-dd'');"
    print(columns[0])
    # # aws_s3 query to export data from RDS to S3
    # cur.execute(f"SELECT * from aws_s3.query_export_to_s3('{query}', '{bucket}', '{tab}.csv', '{region}', 'format csv, DELIMITER ''|''');")
    # print(cur.fetchall())
    # print(f"Data exported successfully from {schema}.{tab} table to {tab}.csv file")

def s3_to_rds():
    # bucket = os.getenv("S3_BUCKET_NAME")
    # region = os.getenv("S3_REGION_NAME")
    bucket = 'doorfeed-preprod-rds-data'
    region = 'eu-west-3'

    # schema = os.getenv("SCHEMA_NAME")
    schema = 'public'

    # tab = os.getenv("RDS_TABLE_NAME")
    ctab = 'copy_spatial_ref_sys'

    # key_id = os.getenv("AWS_ACCESS_KEY_ID")
    key_id = 'AKIAZHWPQOKATLEGQLO5'

    # secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    secret_key = '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'
    # file = os.getenv("S3_FILE_NAME")
    tab = 'spatial_ref_sys'
    file = f'{tab}.csv'

    print(file)

    query = f"SELECT aws_s3.table_import_from_s3('{schema}.{ctab}', '' , '(format csv, DELIMITER ''|'')', aws_commons.create_s3_uri('{bucket}', '{file}','eu-west-3'), aws_commons.create_aws_credentials('{key_id}', '{secret_key}', ''));"
    print(query)
    cur.execute(query)
    print(cur.fetchall())
    # commiting the changes to the database
    conn.commit()
    print("Data imported successfully")



rds_to_s3()
time.sleep(10)
#s3_to_rds()