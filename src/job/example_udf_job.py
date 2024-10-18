from src.common import create_spark_session
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F


@udf(returnType=StringType())
def extract_first_name(name: str) -> str:
    first_name_with_title = name.split(",")[1]
    return first_name_with_title.split(".")[1].lstrip()


@udf(returnType=StringType())
def extract_last_name(name: str) -> str:
    return name.split(",")[0]


@udf(returnType=IntegerType())
def remap_gender(gender: str) -> int:
    return 1 if gender == "male" else 0


def process_titanic_data(spark: SparkSession):
    df = spark.read.csv("../../data/titanic.csv", header=True)

    df = df.withColumn("First_Name", extract_first_name(F.col("Name")))
    df = df.withColumn("Last_Name", extract_last_name(F.col("Name")))
    df = df.withColumn("IsMale", remap_gender(F.col("Sex")))
    df.show(10)


if __name__ == '__main__':
    spark = create_spark_session("udf sample")
    process_titanic_data(spark=spark)
