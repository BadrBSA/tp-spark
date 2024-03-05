import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("spark_clean_job") \
        .master("local[*]") \
        .getOrCreate()
    
    df_city = spark.read.csv("src/resources/exo2/city_zipcode.csv", header=True)
    df_client = spark.read.csv("src/resources/exo2/clients_bdd.csv", header=True)
    
    df_clients_over_18 = filter_column(df_client, 18, "age")
    df_join = join_df(df_clients_over_18, df_city, "zip")
    df_with_columns = get_departments(df_join)
    
    df_with_columns.filter(f.col("departement") == "2B")

    df_with_columns.write.parquet("data/exo2/clean", mode='overwrite')


def filter_column(df, value, col):
    return df.filter(f.col(col) >= value)

def join_df(df, df2, col):
    return df.join(df2, col)

def get_departments(df):
    return df.withColumn('departement', 
    f.when(f.col("zip").substr(1, 2) == 20, f.when(f.col("zip") <= 20190, "2A").otherwise("2B"))
     .otherwise(f.col("zip").substr(1, 2)))