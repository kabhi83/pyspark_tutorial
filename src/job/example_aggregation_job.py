from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from src.common import create_spark_session


def process_data(spark: SparkSession) -> DataFrame:
    data = [("James", "Sales", "NY", 90000, 34, 10000),
            ("Michael", "Sales", "NY", 86000, 56, 20000),
            ("Robert", "Sales", "CA", 81000, 30, 23000),
            ("Maria", "Finance", "CA", 90000, 24, 23000),
            ("Raman", "Finance", "CA", 99000, 40, 24000),
            ("Scott", "Finance", "NY", 83000, 36, 19000),
            ("Jen", "Finance", "NY", 79000, 53, 15000),
            ("Jeff", "Marketing", "CA", 80000, 25, 18000),
            ("Kumar", "Marketing", "NY", 91000, 50, 21000)
            ]
    # Create DataFrame
    schema = ["employee_name", "department", "state", "salary", "age", "bonus"]
    df = spark.createDataFrame(data=data, schema=schema)
    return df


def analyze_aggregation(df: DataFrame):
    df.groupBy("department").count().show(truncate=False)  # number of employees in each department
    df.groupBy("department").sum("salary").show(truncate=False)
    df.groupBy("department").avg("salary").show(truncate=False)
    df.groupBy("department").min("salary").show(truncate=False)
    df.groupBy("department").max("salary").show(truncate=False)

    # GroupBy on multiple columns
    df.groupBy("department", "state").count().show(truncate=False)
    df.groupBy("department", "state").sum("salary", "bonus").show(truncate=False)

    # Running more aggregates at a time
    df.groupBy("department") \
        .agg(
            F.sum("salary").alias("total_salary"),
            F.avg("salary").alias("avg_salary"),
            F.min("salary").alias("min_salary"),
            F.max("salary").alias("max_salary")
    ).show(truncate=False)


if __name__ == '__main__':
    spark = create_spark_session("aggregation sample")
    df = process_data(spark)
    analyze_aggregation(df)

