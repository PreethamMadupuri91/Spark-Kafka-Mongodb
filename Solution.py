import spark

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9091,localhost:9092") \
  .option("subscribe", "test") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df.write.format("mongo").mode("append").save()

