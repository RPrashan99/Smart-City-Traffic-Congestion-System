from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    date_trunc, sum as _sum, col, lit, hour, avg, 
    count, when, round as spark_round, desc
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("DailyPeakHour") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

today = datetime.utcnow().date()
yesterday = today - timedelta(days=1)

print(f"Processing traffic data for: {yesterday}")
print(f"Report date: {today}")

# READ AGGREGATED DATA FROM HDFS
def readAggregatedData(spark, yesterday, today):
    try:
        agg_df = spark.read.parquet("hdfs://namenode:8020/data/traffic/agg_5min") \
            .where(
                (col("window_start") >= f"{yesterday} 00:00:00") &
                (col("window_start") < f"{today} 00:00:00")
            )
        
        record_count = agg_df.count()
        print(f"Loaded {record_count} aggregation records for {yesterday}")
        
        if record_count == 0:
            print("No data found for yesterday. Running with sample data or check pipeline.")
            
    except Exception as e:
        print(f"Error reading data: {e}")
        print("Creating sample data for demonstration...")
        
        sample_schema = StructType([
            StructField("window_start", TimestampType(), True),
            StructField("window_end", TimestampType(), True),
            StructField("sensor_id", StringType(), True),
            StructField("total_vehicles", IntegerType(), True),
            StructField("avg_speed_window", DoubleType(), True),
            StructField("congestion_index", DoubleType(), True)
        ])
        
        # Generate sample data for 4 junctions across 24 hours
        from datetime import datetime as dt
        sample_data = []
        sensors = ["J1", "J2", "J3", "J4"]
        
        for sensor in sensors:
            for hour_val in range(24):
                base_time = dt.strptime(f"{yesterday} {hour_val:02d}:00:00", "%Y-%m-%d %H:%M:%S")
                if hour_val in [8, 9, 17, 18]:
                    vehicles = 120 + (hour_val * 5)
                    speed = 15 + (20 - hour_val % 10)
                else:
                    vehicles = 30 + (hour_val * 2)
                    speed = 35 + (hour_val % 15)
                
                sample_data.append({
                    "window_start": base_time,
                    "window_end": base_time,
                    "sensor_id": sensor,
                    "total_vehicles": vehicles,
                    "avg_speed_window": speed,
                    "congestion_index": vehicles / max(speed, 1)
                })
        
        agg_df = spark.createDataFrame(sample_data, sample_schema)
        print(f"Created {len(sample_data)} sample records for demonstration")

    return agg_df

# CALCULATE HOURLY VOLUMES
def calculateHourlyVolumes(agg_df):
    hourly = agg_df.withColumn("hour", date_trunc("hour", col("window_start"))) \
    .groupBy("sensor_id", "hour") \
    .agg(
        _sum("total_vehicles").alias("vehicles_hour"),
        avg("avg_speed_window").alias("avg_speed_hour")
    )
    print("Calculated hourly volumes")
    return hourly

def findPeakHourPerJunctions(hourly):
    window_spec = Window.partitionBy("sensor_id").orderBy(col("vehicles_hour").desc())

    peak = hourly.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1) \
        .select(
            lit(yesterday).alias("report_date"),
            "sensor_id",
            col("hour").alias("peak_hour"),
            "vehicles_hour",
            spark_round("avg_speed_hour", 2).alias("avg_speed_at_peak")
        ) \
        .withColumn("needs_intervention", 
                    when(col("avg_speed_at_peak") < 15, "YES")
                    .otherwise("NO"))
    
    return peak

def trafficVolumeByHour(agg_df):
    traffic_volume_by_hour = agg_df.withColumn("hour_of_day", hour(col("window_start"))) \
    .groupBy("sensor_id", "hour_of_day") \
    .agg(
        _sum("total_vehicles").alias("total_volume"),
        avg("avg_speed_window").alias("avg_speed"),
        count("*").alias("sample_count")
    ) \
    .orderBy("sensor_id", "hour_of_day")
    return traffic_volume_by_hour

def writeToHDFS(df, path, mode="append", header=True):
    df.coalesce(1) \
    .write \
    .mode(mode) \
    .option("header", str(header).lower()) \
    .csv(f"hdfs://namenode:8020/{path}")

def saveToLocal(peak, traffic_volume_by_hour, filename_peak, filename_volume):
    try:
        peak_pd = peak.toPandas()
        peak_pd.to_csv(filename_peak, index=False)
        print(f"Saved local copy: {filename_peak}")

        volume_pd = traffic_volume_by_hour.toPandas()
        volume_pd.to_csv(filename_volume, index=False)
        print(f"Saved local copy: {filename_volume}")
    except Exception as e:
        print(f"Could not save local files: {e}")

agg_df = readAggregatedData(spark, yesterday, today)
hourly = calculateHourlyVolumes(agg_df)

peak = findPeakHourPerJunctions(hourly)
print("Identified peak hours per junction")

# Display peak hours
print("\n" + "="*60)
print("PEAK HOUR REPORT")
print("="*60)
peak.show(truncate=False)
print("="*60)

# TRAFFIC VOLUME VS TIME OF DAY REPORT
traffic_volume_by_hour = trafficVolumeByHour(agg_df)

print("\nTRAFFIC VOLUME VS TIME OF DAY")
print("="*60)
traffic_volume_by_hour.show(25, truncate=False)
print("="*60)

print("\nPOLICE INTERVENTION RECOMMENDATIONS")
print("="*60)

for row in peak.collect():
    if row['needs_intervention'] == "YES":
        print(f"{row['sensor_id']}: REQUIRES police intervention")
        print(f"   → Peak hour: {row['peak_hour']}")
        print(f"   → Volume: {row['vehicles_hour']} vehicles")
        print(f"   → Speed: {row['avg_speed_at_peak']} km/h (CRITICAL)")
        print(f"   → Recommendation: Deploy 2-3 officers during {row['peak_hour']}\n")
    else:
        print(f"{row['sensor_id']}: Monitor only")
        print(f"   → Peak hour: {row['peak_hour']}")
        print(f"   → Volume: {row['vehicles_hour']} vehicles")
        print(f"   → Speed: {row['avg_speed_at_peak']} km/h (ACCEPTABLE)\n")

print("="*60)

peak.write \
    .mode("append") \
    .parquet("hdfs://namenode:8020/data/traffic/daily_peak_hour")

print("Saved peak hour results to HDFS (Parquet)")



# Save peak results as CSV for report
writeToHDFS(peak, f"reports/peak_hour_report_{yesterday}", mode="overwrite", header=True)
print(f"Saved peak hour CSV report: /reports/peak_hour_report_{yesterday}")


# SAVE TRAFFIC VOLUME VS TIME REPORT
writeToHDFS(traffic_volume_by_hour, f"reports/traffic_volume_vs_time_{yesterday}", mode="overwrite", header=True)
print(f"Saved Traffic Volume vs Time report: /reports/traffic_volume_vs_time_{yesterday}")

# pivot table for easier analysis
pivot_report = traffic_volume_by_hour.groupBy("hour_of_day") \
    .pivot("sensor_id") \
    .agg(_sum("total_volume")) \
    .orderBy("hour_of_day")

writeToHDFS(pivot_report, f"reports/traffic_volume_pivot_{yesterday}", mode="overwrite", header=True)
print(f"Saved pivot table report: /reports/traffic_volume_pivot_{yesterday}")

# GENERATE SUMMARY STATISTICS
print("\nDAILY SUMMARY STATISTICS")
print("="*60)

# Overall statistics
total_vehicles = agg_df.agg(_sum("total_vehicles")).collect()[0][0]
avg_congestion = agg_df.agg(avg("congestion_index")).collect()[0][0]
busiest_junction = peak.orderBy(desc("vehicles_hour")).first()

print(f"Total vehicles across all junctions: {total_vehicles:,}")
print(f"Average congestion index: {avg_congestion:.2f}")
print(f"Busiest junction: {busiest_junction['sensor_id']} with {busiest_junction['vehicles_hour']} vehicles at {busiest_junction['peak_hour']}")
print("="*60)

# EXPORT TO LOCAL FILE SYSTEM
saveToLocal(
    peak, 
    traffic_volume_by_hour, 
    f"peak_hour_report_{yesterday}.csv", 
    f"traffic_volume_vs_time_{yesterday}.csv"
)

print("\n" + "="*60)
print(f"DAILY PROCESSING COMPLETE for {yesterday}")
print("="*60)

spark.stop()