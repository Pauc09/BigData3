# ShopStream ETL PySpark - Punto 3
# Ejecutar: spark-submit shopstream_etl.py --date 2026-05-20 --input-bucket shopstream-datalake-7489 --output-bucket shopstream-datalake-7489

import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--input-bucket", required=True)
    parser.add_argument("--output-bucket", required=True)
    return parser.parse_args()

def create_spark():
    return SparkSession.builder.appName("ShopStream-ETL")\
        .config("spark.sql.adaptive.enabled","true")\
        .config("spark.sql.session.timeZone","UTC")\
        .getOrCreate()

def run(spark, input_bucket, output_bucket, date):
    year, month, day = date.split("-")
    month = month.zfill(2)
    day   = day.zfill(2)
    def path(event_type):
        return f"s3://{input_bucket}/raw/event_type={event_type}/year={year}/month={month}/day={day}/"
    out = f"s3://{output_bucket}/processed/year={year}/month={month}/day={day}"
    pv = spark.read.json(path("page_view"))
    pv = pv.withColumn("timestamp", F.to_timestamp("timestamp"))\
           .withColumn("time_on_page_seconds", F.col("time_on_page_seconds").cast(DoubleType()))\
           .dropDuplicates(["session_id","page_url","timestamp"])\
           .withColumn("time_on_page_seconds",
               F.when(F.col("time_on_page_seconds").isNull(), 30.0)
                .when(F.col("time_on_page_seconds") < 0, 0.0)
                .when(F.col("time_on_page_seconds") > 86400, 86400.0)
                .otherwise(F.col("time_on_page_seconds")))\
           .withColumn("page_type",   F.lower(F.trim("page_type")))\
           .withColumn("device_type", F.lower(F.trim("device_type")))\
           .withColumn("country",     F.upper(F.trim("country")))\
           .filter(F.col("user_id").isNotNull() & F.col("session_id").isNotNull())
    pv.cache()
    try:
        prd = spark.read.json(path("product_view"))\
            .withColumn("timestamp", F.to_timestamp("timestamp"))\
            .withColumn("price", F.col("price").cast(DoubleType()))\
            .withColumn("time_on_page_seconds", F.col("time_on_page_seconds").cast(DoubleType()))\
            .dropDuplicates(["session_id","product_id","timestamp"])\
            .filter(F.col("product_id").isNotNull())
        prd.cache()
    except: prd = None
    try:
        crt = spark.read.json(path("cart_event"))\
            .withColumn("timestamp", F.to_timestamp("timestamp"))\
            .withColumn("action", F.lower(F.trim("action")))\
            .dropDuplicates(["session_id","product_id","action","timestamp"])\
            .filter(F.col("action").isin(["add","remove"]))
        crt.cache()
    except: crt = None
    top_pages = pv.groupBy("page_url","page_type")\
        .agg(F.avg("time_on_page_seconds").alias("avg_time_on_page_seconds"),
             F.count("*").alias("total_views"),
             F.countDistinct("session_id").alias("unique_sessions"))\
        .withColumn("date", F.lit(date))\
        .orderBy(F.desc("avg_time_on_page_seconds")).limit(20)
    spv = pv.groupBy("session_id","page_type").agg(F.count("*").alias("c"))
    total_t  = spv.groupBy("page_type").agg(F.countDistinct("session_id").alias("total"))
    bounce_t = spv.filter(F.col("c")==1).groupBy("page_type").agg(F.countDistinct("session_id").alias("bounces"))
    bounce = total_t.join(bounce_t, on="page_type", how="left").fillna({"bounces":0})\
        .withColumn("bounce_rate", F.round(F.col("bounces")/F.col("total"),4))\
        .withColumn("date", F.lit(date))
    st = pv.groupBy("session_id","user_id").agg(F.sum("time_on_page_seconds").alias("total_time"))
    stats = st.select(F.mean("total_time").alias("m"), F.stddev("total_time").alias("s"),
                      F.expr("percentile_approx(total_time,0.25)").alias("q1"),
                      F.expr("percentile_approx(total_time,0.75)").alias("q3")).collect()[0]
    m,s = float(stats["m"] or 0), float(stats["s"] or 1)
    q1,q3 = float(stats["q1"] or 0), float(stats["q3"] or 0)
    iqr = q3-q1
    anomalies = st\
        .withColumn("z_score", F.abs((F.col("total_time")-m)/s))\
        .withColumn("iqr_out", (F.col("total_time")<q1-1.5*iqr)|(F.col("total_time")>q3+1.5*iqr))\
        .filter((F.col("z_score")>=3.0)|F.col("iqr_out"))\
        .withColumn("anomaly_type", F.lit("high_time_on_page"))\
        .withColumn("metric_value", F.col("total_time"))\
        .withColumn("description", F.concat(F.lit("Tiempo anomalo: "), F.col("total_time").cast(StringType()), F.lit(" seg")))\
        .withColumn("date", F.lit(date))\
        .select("session_id","user_id","anomaly_type","z_score","metric_value","description","date")
    top_pages.write.mode("overwrite").parquet(f"{out}/top_pages_by_time/")
    bounce.write.mode("overwrite").parquet(f"{out}/bounce_rate_by_page_type/")
    anomalies.write.mode("overwrite").parquet(f"{out}/anomalies/")
    pv.unpersist()
    print(f"Pipeline completado para {date}")

if __name__ == "__main__":
    args = parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    run(spark, args.input_bucket, args.output_bucket, args.date)
    spark.stop()
