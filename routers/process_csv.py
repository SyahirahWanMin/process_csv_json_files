from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql import Window
import logging
import os
import sys 
import pandas as pd
from datetime import datetime
import pytz
from delta.tables import DeltaTable
import psycopg2
from sqlalchemy import create_engine

logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

base_dir = os.path.dirname(__file__)  
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../'))
from services.utils import read_data, path_exists, validate_unique_key, cleanse_timestamp, convert_unix_to_dt
from services.utils import apply_mapping, normalize_columns, extract_filename_without_extension, write_data, clean_is_claimed, load_data_to_postgresql

spark = SparkSession.builder.appName("ProcessETLCSV").getOrCreate()

spark = (SparkSession.builder 
    .appName("ProcessETLCSV") 
    .config("spark.executor.memory", "4g") 
    .config("spark.driver.memory", "4g") 
    .config("spark.sql.shuffle.partitions", "200") 
    .getOrCreate())
# conf = spark.sparkContext.getConf() 

job_params = { 
    'input_path': 'src/test.csv',  
    # 'input_path': '/home/wan/process_csv_json_files/src/test.csv',
    'output_path': 's3a://etl-dev/warehouse/sample-test/test',
    'file_format': 'csv',  #can be changed to json, parquet.. etc.
    'delimiter': ',',  
    'quote': '"',  
    'escape': '"',    
    'multiLine': True   
}


db_params = {
    'dbname': 'etl_db',
    'user': 'etluser',
    'password': 'etlpassword',
    'host': 'db',
    'port': '5432'
}


# db_params = {
#     'dbname': os.environ.get('DB_NAME'),
#     'user': os.environ.get('DB_USER'),
#     'password': os.environ.get('DB_PASSWORD'),
#     'host': os.environ.get('DB_HOST'),
#     'port': os.environ.get('DB_PORT'),
# }

# print(f"DB Name: {os.environ.get('DB_NAME')}")
# print(f"DB User: {os.environ.get('DB_USER')}")



def run(*, input_path: str, output_path: str, file_format: str = 'csv', 
        delimiter:str, quote:str, escape:str, multiLine:str ):
    logger.info(f"Started processing raw {file_format} file from {input_path}...")
    # logger.info(f"Current working directory: {os.getcwd()}")
    # logger.info(f"Contents of /app: {os.listdir('/app')}")
    
    # if path_exists(input_path):
    assert path_exists(input_path), f"Input path {input_path} does not exist."
    logger.info("Input file exists, proceeding with reading data...")

    df = read_data(
    input_path=input_path, 
    file_format=file_format, 
    options={
        'header': True,
        'delimiter': delimiter, 
        'quote': quote, 
        'escape': escape, 
        'multiLine': multiLine,
        'inferSchema': True,
        },
        schema=None
    )

    assert df.count() > 0, "DataFrame is empty after loading the data."
    logger.info(f"Loaded data with {df.count()} rows and {len(df.columns)} columns.") 


    logger.info("Performing null value check and uniqueness validation on 'id' column...")
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
    for column in ['id']:
        null_count = null_counts.select(column).collect()[0][0]
        assert null_count == 0, f"Column '{column}' contains null values ({null_count})."

    for column in ['id']:
        try:
            validate_unique_key(df, column)
            print(f"The column '{column}' is unique.")
        except AssertionError:
            print(f"The column '{column}' is NOT unique.")


    df = normalize_columns(df) #use if header is not cleaned  
    df = df.cache()
    df = df.repartition(10).limit(10000) #NOTE: For POC concept 

    # window_spec = Window.orderBy(F.col('id'), F.col('created_at'))

    import uuid
    @F.udf()
    def gen_uuid():
        return str(uuid.uuid4())

    from distutils.util import strtobool

    mapping = {
        'uuid': F.lit(gen_uuid()),
        'id': 'id',
        'name': F.trim('name'),
        'address': F.trim(F.regexp_replace(F.col('address'), "\n", " ")),
        'color': F.initcap(F.trim('color')),
        'created_at_ori': 'created_at',
        'created_at': cleanse_timestamp('created_at').cast(types.TimestampType()),
        'last_login_ori': 'last_login',
        'last_login': convert_unix_to_dt('last_login'),
        'is_claimed_ori': 'is_claimed',
        'is_claimed': clean_is_claimed('is_claimed'),
        'paid_amount': F.col('paid_amount').cast(types.DecimalType(10,2)),
        # 'filename': extract_filename_without_extension(F.input_file_name()),
        'inserted_date': F.lit(datetime.now(tz=pytz.UTC)),
    }

    df = apply_mapping(df, mapping) 
    logger.info("Data transformed with the defined mapping.")

    assert df is not None, "Data mapping failed: The transformed DataFrame is None."
    assert df.count() > 0, "Data mapping failed: The transformed DataFrame is empty."
    
    # # metrics computation
    src_cnt = df.select('id').count()
    if df.head() is None:
        logger.info('No new data to process, nothing to write.')
        metrics = {
            'metrics': {
            'src_count' : 0,
            'dest_count': 0,
            'process_date':datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
    else:
        dest_cnt = df.select('id').count()
        logger.info(f'Total final data records = {dest_cnt}')
        

        # check if data is match before proceeding...
        assert src_cnt == dest_cnt, f"Source count ({src_cnt}) and transformed count ({dest_cnt}) do not match."

        metrics = {
        'metrics': {
        'src_count' : src_cnt,
        'dest_count': dest_cnt,
        'process_date':datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        
    #     # df.filter((F.col('created_at').isNull()) & (F.col('created_at_src') != 'not a date')).select('created_at_src', 'created_at').show(50, truncate=False)
        df.show(5, truncate=False)
        df.printSchema()
        logger.info(f"Metrics calculated: {metrics}")


    # #     # NOTE: Uncomment to save data into S3 
    # #     # write_data(df, output_path, storage_format='delta')
        load_data_to_postgresql(db_params, df, table_name="test")


    return metrics

if __name__ == "__main__":
    try:
        logger.info("Process started")
        metrics = run(**job_params)
        logger.info("Process completed successfully.")
    except Exception as e:
        logger.error(f"Process failed: {e}")




