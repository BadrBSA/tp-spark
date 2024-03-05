import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructField, StructType, StringType, DateType


def main():
    
    spark = SparkSession.builder.appName("exo4").master("local[*]").getOrCreate()

    # on spécifie le schéma de notre dataframe pour être propres
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("date", DateType(), False),
        StructField("category", StringType(), False),
        StructField("price", StringType(), False)
    ])

    df = spark.read.csv('src/resources/exo4/sell.csv', header=True, schema=schema)

    # ajout de la colonne category_name
    df = df.withColumn('category_name', f.when(f.col('category')<6, 'food')
                       .otherwise('furniture'))

    window_cat_per_day = Window.partitionBy(['category_name', 'date']).orderBy('date')

    # ajout de la colonne total_price_per_category_per_day
    df_cat_per_day = df.withColumn('total_price_per_category_per_day',
                                   f.sum('price').over(window_cat_per_day))

    # on se sert du dataframe précédent pour ne garder qu'une ligne par jour
    # et par category_name, afin de pouvoir ensuite prendre les 30 dernieres lignes de la category
    # et joindre cela avec le dataframe de base par rapport à la date et la category_name
    df_cat_per_day_one_line = (df_cat_per_day
                               .select(f.col('date'),
                                       f.col('category_name'),
                                       f.col('total_price_per_category_per_day'))
                               .dropDuplicates())

    # on regarde les 30 dernieres lignes
    window_last_30_days = (Window.partitionBy(['category_name'])
                           .orderBy('date').rowsBetween(start=-30, end=0))

    # on applique la window
    df_cat_per_day_one_line = (df_cat_per_day_one_line
                               .withColumn('total_price_per_category_per_day_last_30_days',
                                           f.sum('total_price_per_category_per_day')
                                           .over(window_last_30_days)))

    # on fait la jointure entre les deux dataframe pour garder les colonnes souhaitées
    df_last_30 = (df.join(df_cat_per_day_one_line, on=['date', 'category_name'], how='left')
                  .select(f.col('id'),
                          f.col('date'),
                          f.col('price'),
                          f.col('category'),
                          f.col('category_name'),
                          f.col('total_price_per_category_per_day'),
                          f.col('total_price_per_category_per_day_last_30_days'))).orderBy(f.col('date'))

    df_last_30.filter(f.col('date') == '2012-01-01').filter(f.col('category_name') == 'furniture')
    df_last_30.filter(f.col('date') == '2012-01-02').filter(f.col('category_name') == 'furniture')
    df_last_30.filter(f.col('date') > '2012-01-02').filter(f.col('category_name') == 'furniture')