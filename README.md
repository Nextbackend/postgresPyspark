# postgresPyspark

### version dependency
- spark 3.5.0
- postgres 16.1
- java 8
- postgres-connector 42.7.0

* ### Add postgresql-42.7.0.jar to lib/pyspark/jars path  

* ### read in batch mode:  
`jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "public.spark_table") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .load()
jdbcDF.show()`  

> jdbc url is jdbc:postgresql://host:port/dbname


* ### write in batch mode:
 ` insert dataframe to postgres in batch mode  
  studentDf = spark.createDataFrame([
    Row(name='amir', age=67),
    Row(name='reza', age=88),
    Row(name='sara', age=79),
    Row(name='raha', age=67),
])`

  ` studentDf.select("name","age").write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "public.spark_table") \
    .option("user", "postgres").option("password", "postgres").mode('append').save()`


* ### write stream:
  * #### note: postgres-spark connector does not support stream write/read
  in this code it implemented with foreachbatch(f) , f represent function that insert micro batch to postgres  
  * run net cat service with nc -lk 9999 and insert data like name , age ( seperated with comma ) (run it before run code)  
   ` streaming_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "9999") \
    .load()`

    #### incoming data from socket has column named 'value' , seperate name and age from this column
`    parsed_stream_df = split_df.selectExpr("data[0] as name", "CAST(data[1] AS INT) as age")
`

`def postgres_writer(microBatch, epoch_id):
    print(epoch_id)
    if microBatch:
        microBatch.write.format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
            .option("driver", "org.postgresql.Driver").option("dbtable", "public.spark_table") \
            .option("user", "postgres").option("password", "postgres").mode('append').save()`
    
    

`    parsed_stream_df.writeStream.outputMode('append').foreachBatch(postgres_writer).start().awaitTermination()
`
