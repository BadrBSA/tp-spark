import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()
    
    print("Hello world!")
    
    df_csv = spark.read.csv("src/resources/exo1/data.csv", header=True)
        
    df_csv = wordcount(df_csv, 'text')
    
    df_csv.write.partitionBy("count").parquet("data/exo1/output", mode='overwrite')
    
        

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()

