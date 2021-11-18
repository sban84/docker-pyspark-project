"""
Simple Spark Application demonstrating running in Docker container
to enter into te docker container console,
docker exec -it <container_id> /bin/bash

-- list the docker process and identify the master process
docker ps

/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--driver-memory 1G \
--executor-memory 1G \
/opt/spark-apps/main.py

"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

"""
Create Spark Session to interact with Spark cluster 
"""


def create_spark():
    spark = SparkSession.builder.appName("docker_spark_app_1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


spark = create_spark()
data = [
    ("a", 200),
    ("a", 100),
    ("b", 100),
    ("d", 100),
    ("b", 500)
]

schema = StructType(
    [
        StructField("name", StringType(), False),
        StructField("salary", StringType(), True),
    ]
)
data_df = spark.createDataFrame(data, schema)
data_df.show(truncate=False)
avg_salary = data_df.groupby(col("name")).agg(avg(col("salary")).alias("avg_sal"))
avg_salary.show(truncate=False)
