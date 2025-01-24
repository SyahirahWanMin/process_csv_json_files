from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, BooleanType
from pyspark.sql import functions as F
from pyspark.sql import types
import logging
import os
import sys
from datetime import datetime
import re
import pytz
from delta.tables import DeltaTable
from sqlalchemy import create_engine

logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

base_dir = os.path.dirname(__file__)  
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../'))
from services.utils import read_data, path_exists, validate_unique_key, cleanse_timestamp, convert_unix_to_dt, adjust_time_format
from services.utils import apply_mapping, normalize_columns, clean_is_claimed, extract_filename_without_extension, write_data, cleanse_tel_num, load_data_to_postgresql

spark = SparkSession.builder.appName("ProcessETLCSV").getOrCreate()

job_params = { 
  'input_path': 'src/test.json', 
  'output_path': 's3a://etl-dev/warehouse/sample-test/test' 
}

db_params = {
    'dbname': 'etl_db',
    'user': 'etluser',
    'password': 'etlpassword',
    'host': 'db',
    'port': '5432'
}



import uuid
@F.udf()
def gen_uuid():
    return str(uuid.uuid4())

def read_data(input_path: str, file_format: str, options: dict , schema: StructType) -> DataFrame:
    logger.info(f"Reading data from {input_path} with file format {file_format}")
    
    read_options = {
        'csv': lambda: spark.read.format('csv').options(**options).load(input_path),
        'json': lambda: spark.read.schema(schema).json(input_path),
        'parquet': lambda: spark.read.parquet(input_path)
    }

    if file_format not in read_options:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    logger.info("Data read successfully")
    return read_options[file_format]()

#original schema (without ETL)
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("logged_at", LongType(), True),
    StructField("car_brand", StringType(), True),
    StructField("car_license_plate", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("user_details", StructType([
        StructField("name", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("address", StringType(), True),
        StructField("username", StringType(), True),
        StructField("password", StringType(), True),
        StructField("national_id", StringType(), True),
        StructField("telephone_numbers", ArrayType(StringType()), True)
    ]), True),
    StructField("jobs_history", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("employer", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("is_fulltime", BooleanType(), True),
        StructField("start", StringType(), True),
        StructField("end", StringType(), True)
    ])), True)
])

def run(*, input_path: str, output_path: str, file_format: str = 'json'):
    logger.info(f"Started processing raw {file_format} file from {input_path}...")
    
    assert path_exists(input_path), f"Input path {input_path} does not exist."
    logger.info("Input file exists, proceeding with reading data...")

    try:
        df = read_data(input_path=input_path, file_format=file_format, options=None, schema=schema)
    except Exception as e:
        logger.error(f"Failed to read data: {e}")
        raise

    assert df.count() > 0, "Input DataFrame is empty."
    
    logger.info("Successfully read data, proceeding with selecting columns")
    user_df = df.select('user_id', 
                        'created_at',
                        'updated_at', 
                        'logged_at', 
                        'user_details.name', 
                        'user_details.dob', 
                        'user_details.address', 
                        'user_details.username',
                        'user_details.password', 
                        'user_details.national_id',
                        'car_brand',
                        'car_license_plate',
                        'is_active',
    )

    logger.info("Processing users details...")
    try:
        user_df = (user_df.withColumn('created_at_str', adjust_time_format(F.col("created_at"))) 
                     .withColumn('updated_at_str', adjust_time_format(F.col("updated_at"))))
    except Exception as e:
        logger.error(f"Failed during column transformation for user_df: {e}")
        raise

    required_columns = ['user_id', 'created_at', 'updated_at', 'logged_at', 'name', 'dob', 'address']
    assert all(col in user_df.columns for col in required_columns), "Missing required columns in user_df."
    
    logger.info("Applying mapping to user_df...")
    user_mapping = {
        'user_id': 'user_id',
        'uuid': F.lit(gen_uuid()),
        'created_at_ori': 'created_at', 
        'created_at': F.unix_timestamp(F.col("created_at_str"), "yyyy-MM-dd HHmm:ss").cast(types.TimestampType()),
        'updated_at_ori': "updated_at",
        'updated_at': F.unix_timestamp(F.col("updated_at_str"), "yyyy-MM-dd HHmm:ss").cast(types.TimestampType()),
        'logged_at_ori': 'logged_at',
        'logged_at': F.from_unixtime('logged_at').cast(types.TimestampType()),
        'name': 'name',
        'dob': F.col('dob').cast(types.DateType()),
        'address': F.trim(F.regexp_replace(F.col('address'), "\n", " ")),
        'username': 'username',
        'password': F.lit("********"),
        'national_id':  F.sha2(F.col('national_id'), 256), 
        'car_brand': 'car_brand', 
        'car_license_plate': 'car_license_plate',
        'is_active': 'is_active',
        'inserted_date': F.lit(datetime.now(tz=pytz.UTC))
    }

    logger.info("Mapping applied to user_df")
    user_df = apply_mapping(user_df, user_mapping)

    # validate_context(user_json)
    
    logger.info("Processing telephone numbers...")
    try:
        tel_num_df = df.select('user_id', 
                            F.explode(F.col('user_details.telephone_numbers'))
                            .alias('telephone_numbers'))

        tel_num_mapping = {
            'user_id': 'user_id',
            'uuid': F.lit(gen_uuid()),
            'telephone_numbers_ori': 'telephone_numbers',
            'telephone_numbers': cleanse_tel_num('telephone_numbers').cast(types.StringType()),
            'inserted_date': F.lit(datetime.now(tz=pytz.UTC))
        }
        
        tel_num_df = apply_mapping(tel_num_df, tel_num_mapping)
    except Exception as e:
        logger.error(f"Failed processing telephone numbers: {e}")
        raise

    assert tel_num_df.count() > 0, "Telephone numbers DataFrame is empty."
    # assert tel_num_df.filter(~F.regexp_extract('uuid', '^[0-9a-fA-F-]{36}$', 0).rlike("^[a-f0-9-]{36}$")).count() == 0, "Invalid UUID format in 'id' column."
    logger.info("Telephone numbers processed successfully.")

    logger.info("Processing job history data...")
    try:
        job_history_df = df.select(
            'user_id',
            F.explode(F.col('jobs_history')).alias('job')
            ).select(
                'user_id',
                'job.id',
                'job.occupation',
                'job.employer',
                'job.is_fulltime',
                'job.start',
                'job.end'
            )
    
        job_history_mapping = {
            'user_id': 'user_id',
            'uuid': F.lit(gen_uuid()),
            'occupation': F.initcap(F.trim('occupation')),
            'employer': F.trim('employer'),
            'is_fulltime': F.col('is_fulltime').cast(types.BooleanType()),
            'start_ori': 'start',
            'start_date': F.col('start').cast(types.DateType()),
            'end_ori': 'end',
            'end_date': F.col('end').cast(types.DateType()),
            'inserted_date': F.lit(datetime.now(tz=pytz.UTC))
        }

        job_history_df = apply_mapping(job_history_df, job_history_mapping)
    except Exception as e:
        logger.error(f"Failed processing job history: {e}")
        raise
    logger.info("Job history processed successfully.")

    # job_history_df.printSchema()
    # tel_num_df.show(10, truncate=False) 
    load_data_to_postgresql(db_params, user_df,  table_name='user_tbl')
    load_data_to_postgresql(db_params, tel_num_df,  table_name='telephone_numbers')
    load_data_to_postgresql(db_params, job_history_df, table_name='jobs_history')

if __name__ == "__main__":
    try:
        logger.info("Process started")
        metrics = run(**job_params)
        logger.info("Process completed successfully.")
    except Exception as e:
        logger.error(f"Process failed: {e}")
