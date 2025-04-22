import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *

## NOTE: change the my sql connector
def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("de_project")\
        .config("spark.driver.extraClassPath", "/Users/mehaksood/spark-3.4.4-bin-hadoop3/mysql-connector-j-9.3.0.jar") \
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark