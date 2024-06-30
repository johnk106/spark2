from pyspark.sql import SparkSession
import os

# Set Python executable paths
os.environ['PYSPARK_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python312/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python312/python.exe'

# Configure Spark session
spark = SparkSession.builder \
    .appName("test") \
    .config("spark.executor.memory", "5g") \
    .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={os.environ['HADOOP_HOME']}") \
    .getOrCreate()

try:
    # Create an RDD
    data = [1, 2, 3, 4, 5]
    rdd = spark.sparkContext.parallelize(data)

    # Perform a simple transformation and action
    result = rdd.map(lambda x: x * x).collect()
    print(result)

except Exception as e:
    print(f"Error occurred: {e}")

finally:
    # Stop the Spark session
    spark.stop()
