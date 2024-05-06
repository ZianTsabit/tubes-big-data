from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SimpleSparkApp") \
        .getOrCreate()

    # Define schema for JSON data
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ])

    # Read JSON data
    input_data_path = "input_data.json"
    input_data = spark.read.json(input_data_path, schema=schema)

    # Show the input data
    input_data.show()

    # Process the data (Example: Filter adults)
    adults_data = input_data.filter(col("age") >= 18)

    # Show the processed data
    adults_data.show()

    # Stop Spark session
    spark.stop()