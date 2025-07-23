from pyspark.sql import SparkSession

def get_spark_session(app_name="DataPipelineApp"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def load_csv_data(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)
