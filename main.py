import time
from pyspark.sql.functions import max

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.config("spark.jars", "/home/amirhosein/Downloads/postgresql-42.7.0.jar")\
    .master("local").appName("PySpark_Postgres").getOrCreate()
#
# jdbcDF = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#     .option("dbtable", "public.spark_table") \
#     .option("user", "postgres") \
#     .option("password", "postgres") \
#     .load()
# jdbcDF.show()

# __________________________________________________________________________________________________
# studentDf = spark.createDataFrame([
#     Row(name='amir', age=67),
#     Row(name='reza', age=88),
#     Row(name='sara', age=79),
#     Row(name='raha', age=67),
# ])
#
# studentDf.select("name","age").write.format("jdbc")\
#     .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#     .option("driver", "org.postgresql.Driver").option("dbtable", "public.spark_table") \
#     .option("user", "postgres").option("password", "postgres").mode('append').save()

# _______________________________________________________________________________________________________

# streaming_df = spark.readStream \
#     .format("socket") \
#     .option("host", "localhost") \
#     .option("port", "9999") \
#     .load()
#
#
# split_df = streaming_df.select(split(col("value"), ",").alias("data"))
#
# # Extract "name" and "age" columns
# parsed_stream_df = split_df.selectExpr("data[0] as name", "CAST(data[1] AS INT) as age")
#
#
# # parsed_stream_df.writeStream.format('console').start().awaitTermination()
# def postgres_writer(microBatch, epoch_id):
#     print(epoch_id)
#     if microBatch:
#         microBatch.write.format("jdbc") \
#             .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#             .option("driver", "org.postgresql.Driver").option("dbtable", "public.spark_table") \
#             .option("user", "postgres").option("password", "postgres").mode('append').save()
#
#
# parsed_stream_df.writeStream.outputMode('append').foreachBatch(postgres_writer).start().awaitTermination()


# _____________________________________________________________________________________________________________

