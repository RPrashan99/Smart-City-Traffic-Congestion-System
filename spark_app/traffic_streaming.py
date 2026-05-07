from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum as _sum, avg as _avg, 
    expr, lit, to_json, struct, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    TimestampType, IntegerType, DoubleType
)

class TrafficStreamingApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SmartCityTrafficStreaming") \
            .config("spark.master", "spark://spark-master:7077") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()
        
        self.spark.conf.set("spark.sql.shuffle.partitions", "4")
        self.spark.sparkContext.setLogLevel("WARN")

        self.schema = StructType([
            StructField("sensor_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("vehicle_count", IntegerType(), False),
            StructField("avg_speed", DoubleType(), False)
        ])

    def read_from_kafka(self, topic="traffic_raw"):
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str") \
            .select(from_json(col("json_str"), self.schema).alias("data")) \
            .select(
                col("data.sensor_id"),
                col("data.vehicle_count"),
                col("data.avg_speed"),
                col("data.timestamp").cast(TimestampType()).alias("event_time")
            ) \
            .withWatermark("event_time", "10 minutes")
        
        return parsed_df
    
    def calculate_congestion_index(self, parsed_df):
        agg_df = parsed_df \
            .groupBy(
                window(col("event_time"), "5 minutes"),
                col("sensor_id")
            ) \
            .agg(
                _sum("vehicle_count").alias("total_vehicles"),
                _avg("avg_speed").alias("avg_speed_window")
            ) \
            .withColumn(
                "congestion_index",
                expr("total_vehicles / nullif(avg_speed_window, 0.0)")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "sensor_id",
                "total_vehicles",
                "avg_speed_window",
                "congestion_index"
            )
        
        return agg_df
    
    def detect_congestion_alerts(self, parsed_df):
        alerts_df = parsed_df \
            .filter(col("avg_speed") < 10.0) \
            .withColumn("alert_time", current_timestamp()) \
            .withColumn("reason", lit("avg_speed_below_10_kmph")) \
            .select(
                col("alert_time"),
                col("event_time").alias("detected_at"),
                "sensor_id",
                "vehicle_count",
                "avg_speed",
                "reason"
            )
        
        return alerts_df
    
    def write_to_hdfs(self, df, path, checkpoint_path, trigger_interval):
        return df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", path) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime=trigger_interval) \
            .start()

    def write_to_kafka(self, df, topic="critical-alerts", checkpoint_path="hdfs://namenode:8020/checkpoints/alert-kafka"):
        return df.select(
            to_json(struct("*")).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("append") \
            .trigger(processingTime='5 seconds') \
            .start()

    def write_to_console(self, df, trigger_interval='30 seconds'):
        return df.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 10) \
            .trigger(processingTime=trigger_interval) \
            .start()
    
    def run_streaming_pipeline(self):
        parsed_df = self.read_from_kafka()
        agg_df = self.calculate_congestion_index(parsed_df)

        # write aggregated results to HDFS every 1 minute
        self.write_to_hdfs(
            agg_df, 
            "hdfs://namenode:8020/data/traffic/agg_5min", 
            "hdfs://namenode:8020/checkpoints/traffic_agg_5min", 
            '1 minute'
        )
        print("Aggregation HDFS stream started")

        alerts_df = self.detect_congestion_alerts(parsed_df)
        self.write_to_hdfs(
            alerts_df,
            "hdfs://namenode:8020/data/traffic/alerts",
            "hdfs://namenode:8020/checkpoints/traffic_alerts",
            '10 seconds'
        )
        print("Alert HDFS stream started")

        self.write_to_kafka(alerts_df)
        print("Alert Kafka stream started")

        self.write_to_console(agg_df)
        print("Console stream started")

        self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    app = TrafficStreamingApp()
    app.run_streaming_pipeline()