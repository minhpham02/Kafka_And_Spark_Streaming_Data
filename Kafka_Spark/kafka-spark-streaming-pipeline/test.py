from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("SimpleSparkTest").getOrCreate()

# Sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]

# Define the schema
schema = ["name", "age"]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

# Count the number of rows in the DataFrame
row_count = df.count()
print(f"Number of rows: {row_count}")

# Stop the SparkSession
spark.stop()
