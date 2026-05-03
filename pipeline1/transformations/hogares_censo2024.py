from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.active()


@dp.materialized_view(name="hogares")
def hogares_censo2024() -> DataFrame:
    df = spark.read.parquet("data/hogares_censo2024.parquet")
    return df


@dp.temporary_view(name="hogares_per_region_view")
def hogares_per_region() -> DataFrame:
    df = spark.table("hogares") \
        .groupBy("region") \
        .count() \
        .withColumnRenamed("count", "num_hogares")

    return df


@dp.materialized_view(name="hogares_per_region")
def hogares_per_region_table() -> DataFrame:
    return spark.sql("SELECT * FROM hogares_per_region_view")
