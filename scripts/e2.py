"""
2. How to convert the index of a PySpark DataFrame into a column?
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


# create spark session
spark = SparkSession.builder.appName("exercise 2").getOrCreate()

# init df
df = spark.createDataFrame([
("Alice", 1),
("Bob", 2),
("Charlie", 3),
], ["Name", "Value"])

# create a column namely index that shows row ids
df = df.withColumn("index", F.monotonically_increasing_id())

df.show()
