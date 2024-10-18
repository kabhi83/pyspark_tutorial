from pyspark import SparkConf
from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    conf = (
        SparkConf()
        .set("spark.driver.memory", "8g")
        .set("spark.sql.session.timeZone", "UTC")
    )

    spark = SparkSession.builder\
        .master("local[*]")\
        .config(conf=conf)\
        .appName(app_name)\
        .getOrCreate()

    return spark
