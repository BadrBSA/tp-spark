import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("spark_clean_job") \
        .master("local[*]") \
        .getOrCreate()
        
    df_clean = spark.read.parquet("data/exo2/clean")
        
    df = get_count_per_departments(df_clean, "count", "departement")
    
    df.write.csv("data/exo2/aggregate", mode='overwrite')
    
def get_count_per_departments(df, order, order2):
    return df.groupBy("departement").count().orderBy(order, order2, ascending=[True, False])



