from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, to_date, month, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Trip Analysis").getOrCreate()
df = spark.read.csv("/opt/bitnami/spark/jobs/data.csv", header=True, inferSchema=True)

def avg_duration_per_day():
    df_day = df.groupBy("date").agg(avg("duration").alias("avg_duration"))
    df_day.write.csv("/opt/bitnami/spark/jobs/out/avg_duration_per_day", header=True, mode="overwrite")

def trip_count_per_day():
    df_count = df.groupBy("date").agg(count("*").alias("trip_count"))
    df_count.write.csv("/opt/bitnami/spark/jobs/out/trip_count_per_day", header=True, mode="overwrite")

def top_start_station_per_month():
    df_month = df.withColumn("month", month(to_date("date")))
    window = Window.partitionBy("month").orderBy(desc("count"))
    df_count = df_month.groupBy("month", "start_station").agg(count("*").alias("count"))
    df_top = df_count.withColumn("rank", row_number().over(window)).filter(col("rank") == 1)
    df_top.write.csv("/opt/bitnami/spark/jobs/out/top_station_per_month", header=True, mode="overwrite")

def top_3_stations_last_2_weeks():
    last_2_weeks = df.orderBy(desc("date")).limit(14)
    df_grouped = last_2_weeks.groupBy("date", "start_station").agg(count("*").alias("trip_count"))
    window = Window.partitionBy("date").orderBy(desc("trip_count"))
    df_top3 = df_grouped.withColumn("rank", row_number().over(window)).filter(col("rank") <= 3)
    df_top3.write.csv("/opt/bitnami/spark/jobs/out/top3_per_day_last_2_weeks", header=True, mode="overwrite")

def gender_avg_duration():
    df_gender = df.groupBy("gender").agg(avg("duration").alias("avg_duration"))
    df_gender.write.csv("/opt/bitnami/spark/jobs/out/gender_avg_duration", header=True, mode="overwrite")

avg_duration_per_day()
trip_count_per_day()
top_start_station_per_month()
top_3_stations_last_2_weeks()
gender_avg_duration()

spark.stop()
