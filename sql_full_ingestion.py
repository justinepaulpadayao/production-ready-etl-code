import psycopg2
from psycopg2 import Error
import csv
import boto3
import configparser

# Extraction
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

try:
    # Connect to RDS PostgreSQL database
    connection = psycopg2.connect(
        user=user,
        password=password,
        database=dbname,
        host=host,
        port=port
    )

    # Create a cursor to perform database operations
    cursor = connection.cursor()

    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")

    # Confirm connection with PostgreSQL and print version details
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print("You are connected to -", version, "\n")

    # Executing the full extraction query
    query = "SELECT * FROM Orders;"
    cursor.execute(query)

    # Fetch result
    results = cursor.fetchall()
    print(f"There are {len(results)} rows extracted.", "\n")

    # Writing to csv
    local_filename = "order_extract.csv"
    with open(local_filename, 'w') as fp:
        csv_w = csv.writer(fp, delimiter='|')
        csv_w.writerows(results)
    print("Successfully written files to CSV format.", "\n")
    fp.close()

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed.", "\n")


# Loading
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# Connecting to S3 resource
s3 = boto3.client('s3',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key)

# Uploading to S3 bucket
s3_file = local_filename
s3.upload_file(local_filename, bucket_name, s3_file)
print(f"Successfully uploaded {s3_file} into {bucket_name}.")
