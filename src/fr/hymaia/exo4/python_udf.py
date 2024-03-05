import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType



def main():

    spark = SparkSession.builder.appName("exo4").master("local[*]").getOrCreate()

    add_category_udf = f.udf(add_category_name, StringType())

    df = spark.read.csv('src/resources/exo4/sell.csv', header=True)

    df = df.withColumn('category_name', add_category_udf(col('category')))
        
def add_category_name(value):
    if int(value) < 6:
        return 'food'
    else:
        return 'furniture'