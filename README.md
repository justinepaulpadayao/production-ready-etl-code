# Production-Ready Python ETL Code

This is a collection of sample template for ETL pipelines.

##### Extracting Data from a PostgreSQL Database

Prerequisites
1. Install pyscopg2: 
    ``` 
    pip install pyscopg2 
    ```
2. Add new section to pipeline.conf
    ``` 
    [postgres_config]
    host = myhost.com
    port = 5432
    username = my_username
    password = my_password
    database = db_name 
    ``` 
