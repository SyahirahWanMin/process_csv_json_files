from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Column
from pyspark.sql import types
from io import BytesIO
from datetime import datetime, timedelta
import os
import re
import logging
# from pydantic import BaseModel, ValidationError
from typing import List, Optional, Dict, Union
from sqlalchemy import create_engine

spark = SparkSession.builder.appName("ProcessData").getOrCreate()
# s3 = boto3.client('s3')
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass

def path_exists(path: str) -> bool:
    return os.path.exists(path)

#extendable to allow ingestion of new raw data sources
#usage: for flat files
def read_data(input_path: str, file_format: str, options: dict, schema) -> DataFrame:
    read_options = {
        'csv': lambda: spark.read.format('csv').options(**options).load(input_path),
        'json': lambda: spark.read.json(input_path, schema),
        'parquet': lambda: spark.read.parquet(input_path)
    }

    if file_format not in read_options:
        raise ValueError(f"Unsupported file format: {file_format}")
    return read_options[file_format]()


# Additional: For S3, utilizing boto3 
def download_csv_from_s3(bucket_name, file_name):
    try:
        s3_object = s3.get_object(Bucket=bucket_name, Key=file_name)
        csv_data = s3_object['Body'].read()
        return BytesIO(csv_data)  
    except Exception as e:
        raise Exception(f"Failed to download CSV file from S3 bucket '{bucket_name}' with key '{file_name}': {str(e)}")

@F.udf(returnType=types.StringType())
def extract_filename_without_extension(name: str):
  name = name.split('/')[-1]
  return '.'.join(name.split('.')[:-1])

def validate_unique_key(df: DataFrame, cols):
    """ Sanity check to test whether specified columns can be used as unique/primary key"""
    if isinstance(cols, str):
       cols = [F.col(cols)]
    elif not hasattr(cols, '__iter__'):
       cols = [cols]
       
    q: DataFrame = df.groupBy(cols).count().where(F.col('count') > 1)
    if q.count() > 0:
       raise AssertionError("key is not unique") 
    
@F.udf(returnType=types.BooleanType())
def clean_is_claimed(value):
    if value is None:
        return None
    
    replacements = [
        ("truee", "true"),
        ("fal_se", "false"),
    ]
    value = str(value).lower()  
    for old, new in replacements:
        value = value.replace(old, new)

    if value == "true":
        return True
    elif value == "false":
        return False
    return None


def apply_mapping(df: DataFrame, mapping: dict, extend: bool=False) -> DataFrame:
    """
    Apply data transformation mapping. Mapping must be a key value
    pair where key is the output alias, and value can be either the column name in
    string, or a function that takes dataframe as first parameter, and returns column
    object
    """

    select: list[Column] = []
    aliases: dict = {}
    for k,v in mapping.items():
        if isinstance(v, str):
            op = F.col(v).alias(k)
        elif callable(v):
            op = v(df).alias(k)
        elif isinstance(v, Column):
            op = v.alias(k)
        else:
            raise ValueError("Unknown mapping operation %s" % v)
        select.append(op)
        aliases[k.lower()] = op

    if not extend:
        return df.select(*select)
    else:
        existing = []
        select_cols = []
        for c in df.columns:
            if c.lower() not in aliases.keys():
                select_cols.append(F.col(c))
            else:
                select_cols.append(aliases[c.lower()])
                existing.append(c.lower())
                
        for k,v  in aliases.items():
            if k not in existing:
                select_cols.append(v)
        return df.select(*select_cols)

def normalize_columns(df: DataFrame) -> DataFrame:
    """
    Cleanup column name
    """
    # Remove "(dd/mm/yyyy)" and surrounding spaces from the name
    select = []
    for c in df.columns:
        name = re.sub(r'\(dd/mm/yyyy\)', '', c)
        name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        name = re.sub(r'_+', r'_', name)
        name = name.lower().strip('_')
        select.append(F.col(c).alias(name))
    return df.select(*select)



def cleanse_timestamp(col: str):
    min_date = "1900-01-01 00:00:00"
    max_date = "2029-01-31 00:00:00"
    return (
        F.when(F.col(col).rlike(r'not a date'), F.lit(None)) 
        .when(
            F.col(col).rlike(r'^[A-Za-z]+,\s[A-Za-z]+\s\d{1,2}[a-z]{2},\s\d{4}$'),
            F.to_timestamp(
                F.regexp_replace(
                    F.regexp_replace(F.regexp_replace(F.col(col), r'^[A-Za-z]+,\s', ''),  
                    r'(\d{1,2})(st|nd|rd|th)', r'\1'), 
                    r'\s+', ' '  
                ),
                "MMMM d, yyyy"  
            )
        )
        .when(
            F.col(col).rlike(r'^[A-Za-z]+\s\d{1,2}[a-z]{2},\s\d{4}$'),
            F.to_timestamp(F.regexp_replace(F.col(col), r'(\d{1,2})(st|nd|rd|th)', r'\1'), "MMMM d, yyyy")
        )
        .when(
            F.col(col).rlike(r'^\d{4}-\d{2}-\d{2}TEST$'),
            F.to_timestamp(F.substring(F.col(col), 1, 10), "yyyy-MM-dd")
        )
        .when(
            F.col(col).cast("date") < min_date,
            F.lit(min_date)  
        )
        .when(
            F.col(col).cast("date") > max_date,
            F.lit(max_date)  
        )
        .otherwise(F.col(col))  
    )

def convert_unix_to_dt(col: str):
    return F.from_unixtime(F.col(col).cast("long")).cast("timestamp")

@F.udf
def clean_identity_num(name: str):
    if not name:
        return None
    name = name.strip()
    name = name.strip('\t')
    name = re.sub(r'-', '', name)
    name = re.sub(r'\<br\s*\/\>', '', name)
    return name

# adjust_time_format_udf = F.udf(lambda x: adjust_time_format(x) if x else None, StringType())
@F.udf(returnType=types.StringType())
def adjust_time_format(time_str: str):
    try:
        if re.match(r"^\d{9,10}$", time_str):  
            return datetime.utcfromtimestamp(int(time_str)).strftime('%Y-%m-%d %H:%M:%S')

        match_malformed = re.match(r"(\d{4}-\d{2}-\d{2}) (\d{2})(\d{2})(\d{2})", time_str)  # Note the space between date and time
        if match_malformed:
            date_part = match_malformed.group(1)  
            hour = match_malformed.group(2)  
            minute = match_malformed.group(3) 
            second = match_malformed.group(4)  
            return f"{date_part} {hour}:{minute}:{second}"

        match_malformed_no_seconds = re.match(r"(\d{4}-\d{2}-\d{2}T)(\d{2})(\d{2})", time_str)
        if match_malformed_no_seconds:
            date_part = match_malformed_no_seconds.group(1) 
            hour = match_malformed_no_seconds.group(2)  
            minute = match_malformed_no_seconds.group(3)  
            return f"{date_part}{hour}:{minute}:00".replace('T', ' ')

        match_iso = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}):(\d{2}):(\d{2})", time_str)
        if match_iso:
            date_part, minute, sec = match_iso.groups()
            minute = int(minute)
            sec = int(sec)

            if sec >= 60:
                minute += sec // 60
                sec = sec % 60
            if minute >= 60:
                hour = int(date_part[11:13]) + minute // 60
                minute = minute % 60

                base_date = datetime.strptime(date_part[:10], "%Y-%m-%d")
                base_time = timedelta(hours=hour)
                adjusted_datetime = base_date + base_time

                return adjusted_datetime.strftime('%Y-%m-%d %H:%M:%S')

            return f"{date_part[:14]}{int(minute):02d}:{int(sec):02d}".replace('T', ' ')
        return time_str

    except Exception as e:
        logger.error(f"Error in adjust_time_format: {e}")
        return time_str


def accumulate_pat_reps(col: Column, pat_reps: list) -> Column:
    from functools import reduce
    pat = lambda p: p[0]
    rep = lambda p: p[1]
    accumulator = lambda col, pat_rep: F.regexp_replace(col, pat(pat_rep), rep(pat_rep))
    return reduce(accumulator, pat_reps, col)


def cleanse_tel_num(col: F.Column) -> F.Column:
    """
    Cleanses and formats a phone number column.
    
    Steps:
    - Removes spaces.
    - Removes prefixes like +1 or 001 (international codes).
    - Removes extensions (e.g., x1234).
    - Retains only digits.
    - Validates and formats valid phone numbers (10 digits).
    """
    pat_reps = [
        (r'\s+', ''),            
        (r'^\+1|^001', ''),            
        (r'x\d*', ''),             
        (r'[^\d]', ''),           
    ]

    col = accumulate_pat_reps(col, pat_reps)
    col = F.when(F.length(col) == 10, col).otherwise(F.lit(None))
    return col

def load_to_postgres_via_cursor(df, db_params, table_name: str):
    try:
        engine = create_engine(
            f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["dbname"]}'
        )

        with engine.connect() as connection:
            with connection.begin():
                with connection.connection.cursor() as cursor:
                    temp_table = f"{table_name}_temp"
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table};")

                    df_pd = df.toPandas()  

                    df_pd.head(0).to_sql(
                        temp_table,
                        connection,
                        index=False,
                        if_exists='replace'
                    )

                    from io import StringIO
                    buffer = StringIO()
                    df_pd.to_csv(buffer, header=False, index=False)
                    buffer.seek(0)

                    cursor.copy_expert(
                        f"COPY {temp_table} FROM STDIN WITH CSV NULL AS ''", buffer
                    )

                    cursor.execute(f"""
                        DELETE FROM {table_name};
                        INSERT INTO {table_name} SELECT * FROM {temp_table};
                        DROP TABLE {temp_table};
                    """)

                    logger.info(f"Data loaded into PostgreSQL table: {table_name}")

    except Exception as e:
        logger.error(f"Failed to load data into PostgreSQL: {e}")
        raise

#write data to S3 or local 
def write_data(df: DataFrame, output_path: str, storage_format: str) -> None:
    logger.info(f"Writing data to {output_path} in {storage_format} format...")

    if storage_format == 'delta':
        df.write.format('delta').mode('append').save(output_path)
    elif storage_format == 'parquet':
        df.write.format('parquet').mode('append').save(output_path)
    else:
        logger.warning(f"Unsupported storage format '{storage_format}', defaulting to CSV.")
        df.write.format('csv').option("header", True).mode('overwrite').save(output_path)
    logger.info(f"Written {df.count()} rows to {output_path}.")


# def validate_context(_ctx: dict):
#     class TelephoneNumber(BaseModel):
#         user_id: str
#         id: str
#         telephone_numbers: str
#         inserted_date: Optional[str]

#     # ctx = Context(**_ctx)
#     # return ctx
#     try:
#         ctx = TelephoneNumber(**_ctx)
#         return ctx
#     except ValidationError as e:
#         raise Exception(f"Validation error in context: {e}")


def load_data_to_postgresql(db_params, df, table_name, if_exists="replace"):
    """
    Loads a Spark DataFrame to a PostgreSQL table.

    Args:
        db_params (dict): Dictionary containing PostgreSQL connection parameters:
                          user, password, host, port, and dbname.
        df (pyspark.sql.DataFrame): The Spark DataFrame to load.
        table_name (str): The name of the target table in the database.
        if_exists (str): Behavior if the table already exists.
                         Options: 'fail', 'replace', 'append'. Default is 'replace'.
    """
    try:
        # Create SQLAlchemy engine
        engine = create_engine(
            f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@'
            f'{db_params["host"]}:{db_params["port"]}/{db_params["dbname"]}'
        )

        # Convert Spark DataFrame to Pandas DataFrame
        df_pandas = df.toPandas()

        import pandas as pd
        # df_pandas = df_pandas.fillna({
        #     'paid_amount': 0.0,  # Replace NaN in 'paid_amount' with 0.0
        #     'is_claimed': False, # Replace NaN in 'is_claimed' with False
        #     'last_login_ori': 0  
        # })

        # Load data into PostgreSQL table
        df_pandas.to_sql(table_name, engine, index=False, if_exists=if_exists)
        logger.info(f"Data successfully loaded into PostgreSQL table '{table_name}'.")
    
    except Exception as e:
        logger.error(f"Failed to load data into PostgreSQL: {e}")
        raise