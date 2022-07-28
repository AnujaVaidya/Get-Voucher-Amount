from flask import Flask, request
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import json

from pyspark.sql.functions import when, datediff, col,max

app = Flask(__name__)


@app.route('/post_json', methods=['POST'])
def process_json():
    content_type = request.headers.get('Content-Type')
    if content_type == 'application/json':
        resp_json = request.json

        segment = resp_json["segment_name"]
        country = resp_json["country_code"]
        last_order = resp_json["last_order_ts"]
        first_order = resp_json["first_order_ts"]

        spark = SparkSession \
            .builder \
            .appName("SparkTesting") \
            .getOrCreate()

        df = spark.read \
            .option("compression", "gzip") \
            .parquet("data.parquet.gzip")

        # Making total_orders and voucher_amount to 0.00 in case of null/empty values
        df = df.filter(df.country_code == country)
        df = df.withColumn("total_orders",
                           when((df.voucher_amount.isNull()) | (df.voucher_amount == "") | (df.voucher_amount == " "),
                                "0")
                           .otherwise(df.voucher_amount))
        df = df.withColumn("voucher_amount", when(df.voucher_amount.isNull(), "0.00")
                           .otherwise(df.voucher_amount))

        # Set frequency segment column based on total_orders placed by a customer. Each row is one customer data as
        # mentioned in documentation
        df = df.withColumn('frequency_segment',
                           f.when((f.col('total_orders') >= 0) & (f.col('total_orders') <= 4),
                                  f.lit('frequent_segment'))
                           .when((f.col('total_orders') > 4) & (f.col('total_orders') <= 13), f.lit('frequent_segment'))
                           .when((f.col('total_orders') > 13) & (f.col('total_orders') <= 37),
                                 f.lit('frequent_segment'))
                           .otherwise(None))

        # Set regency segment by calculating the days/time difference between timestamp and last order
        df = df.withColumn("recency_segment", when((datediff(col("timestamp"), col("last_order_ts")) > 30) &
                                                   (datediff(col("timestamp"), col("last_order_ts")) <= 60),
                                                   "recency_segment")
                           .when((datediff(col("timestamp"), col("last_order_ts")) > 60) &
                                 (datediff(col("timestamp"), col("last_order_ts")) <= 90), "recency_segment")
                           .when((datediff(col("timestamp"), col("last_order_ts")) > 90) &
                                 (datediff(col("timestamp"), col("last_order_ts")) <= 120), "recency_segment")
                           .when((datediff(col("timestamp"), col("last_order_ts")) > 120) &
                                 (datediff(col("timestamp"), col("last_order_ts")) < 180), "recency_segment")
                           .when(datediff(col("timestamp"), col("last_order_ts")) > 180, "recency_segment")
                           .otherwise(None)
                           )
        # Check which segment is passed in the API request
        if segment == "recency_segment":
            df = df.filter((df.last_order_ts <= last_order) & (df.first_order_ts >= first_order)
                           & (df.last_order_ts >= first_order) & (df.first_order_ts <= last_order) & (
                                       df.recency_segment == "recency_segment"))
        elif segment == "frequency_segment":
            df = df.filter((df.last_order_ts <= last_order) & (df.first_order_ts >= first_order)
                           & (df.last_order_ts >= first_order) & (df.first_order_ts <= last_order) & (
                                   df.recency_segment == "frequency_segment"))

        # Group all the filtered df's voucher amount and return the maximum value found
        df1 = df.groupby('voucher_amount').count()
        val = {
            "voucher_amount": df1.select(max("count")).collect()[0][0]
        }
        return json.dumps(val)

    else:
        return 'Content-Type not supported!'


if __name__ == "__main__":
    app.run(port=8000)
