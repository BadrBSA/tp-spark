from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_aggregate_job import get_count_per_departments
from src.fr.hymaia.exo2.spark_clean_job import filter_column, join_df, get_departments
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType


class TestExo2(unittest.TestCase):
    
    def test_filtercolumn(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(text='toto', value=5),
                Row(text='titi', value=1)
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(text='toto', value=5)
            ]
        )
        
        not_expected = spark.createDataFrame(
            [
                Row(text='titi', value=1)
            ]
        )

        actual = filter_column(input, 5, "value")
                
        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.schema, expected.schema)
        
        self.assertNotEqual(actual.collect(), not_expected.collect())
    
    
    
    def test_join_df(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(text='toto', value=5),
                Row(text='titi', value=1)
            ]
        )
        input_2 = spark.createDataFrame(
            [
                Row(text='toto', age=25),
                Row(text='titi', age=18)
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(text='toto', value=5, age=25),
                Row(text='titi', value=1, age=18)
            ]
        )
        
        not_expected = spark.createDataFrame(
            [
                Row(text='toto', value=5),
                Row(text='titi', value=1)            
            ]
        )

        actual = join_df(input, input_2, "text")
                
        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.schema, expected.schema)

        self.assertNotEqual(actual.collect(), not_expected.collect())
    
    
    
    def test_get_departments(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(zip="25650"),
                Row(zip="20190"),
                Row(zip="20191")
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(zip="25650", departement="25"),
                Row(zip="20190", departement="2A"),
                Row(zip="20191", departement="2B")
            ]
        )
        
        not_expected = spark.createDataFrame(
            [
                Row(zip="25650", departement="25"),
                Row(zip="20190", departement="2A"),
                Row(zip="20191", departement="2A")         
            ]
        )

        actual = get_departments(input)
                
        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.schema, expected.schema)

        self.assertNotEqual(actual.collect(), not_expected.collect())

 
    def test_get_count_per_departments(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(departement='toto'),
                Row(departement='titi'),
                Row(departement='tata'),
                Row(departement='toto'),
                Row(departement='titi'),
            ]
        )
        
        schema = StructType([
        StructField("departement", StringType(), True),
        StructField("count", LongType(), False)
    ])
        
        expected = spark.createDataFrame(
            [
                Row(departement='tata', count=1),
                Row(departement='toto', count=2),
                Row(departement='titi', count=2)
            ],
            schema=schema
        )
        
        not_expected = spark.createDataFrame(
            [
                Row(departement='titi', count=2),
                Row(departement='toto', count=2),
                Row(departement='tata', count=1)  
            ],
            schema=schema
        )

        actual = get_count_per_departments(input, "count", "departement")
                
        self.assertEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.schema, expected.schema)

        self.assertNotEqual(actual.collect(), not_expected.collect())

    def test_clean_job(self):
        df_client = spark.createDataFrame(
            [
                Row(text='toto', key=5, age=25,zip="20190", departement="2A"),
                Row(text='titi', key=1, age=22, zip="20191", departement="2B"),
                Row(text='tata', key=2, age=46,zip="25650", departement="25"),
                Row(text='tete', key=3, age=28, zip="25650", departement="25"),
                Row(text='test', key=4, age=17, zip="25650", departement="25")
            ]
        )

        df_name = spark.createDataFrame(
            [
                Row(key=5, last_name="aha"),
                Row(key=1, last_name="oho"),
                Row(key=2, last_name="ihi"),
                Row(key=3, last_name="ehe"),
                Row(key=4, last_name="aya")
            ]
        )
        
        expected = spark.createDataFrame(
            [
                Row(key=1, text="titi", age=22, zip='20191', departement="2B", last_name="oho"),
                Row(key=2, text="tata", age=46, zip='25650', departement="25", last_name="ihi"),
                Row(key=3, text="tete", age=28, zip='25650', departement="25", last_name="ehe"),
                Row(key=5, text="toto", age=25, zip='20190', departement="2A", last_name="aha")
            ]
        )
        
        not_expected = spark.createDataFrame(
            [
                Row(key=1, text="titi", age=22, zip='20191', departement="2A", last_name="oho"),
                Row(key=2, text="tata", age=46, zip='25650', departement="25", last_name="ihi"),
                Row(key=3, text="tete", age=28, zip='25650', departement="25", last_name="ehe"),
                Row(key=5, text="toto", age=25, zip='20190', departement="2A", last_name="aha")
            ]
        )
        
        df_clients_over_18 = filter_column(df_client, 18, "age")
        df_join = join_df(df_clients_over_18, df_name, "key")
        actual = get_departments(df_join)
        
        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.schema, expected.schema)

        self.assertNotEqual(actual.collect(), not_expected.collect())
        
        with self.assertRaises(Exception):
            df_clients_over_18 = filter_column(df_client, 18, "age")
            df_join = join_df(df_clients_over_18, df_name, 1)
            actual = get_departments(df_join)
    
        
    def test_aggregate_job(self):
        input = spark.createDataFrame(
            [
                Row(key=1, text="titi", age=22, zip='20191', departement="2B", last_name="oho"),
                Row(key=2, text="tata", age=46, zip='25650', departement="25", last_name="ihi"),
                Row(key=3, text="tete", age=28, zip='25650', departement="25", last_name="ehe"),
                Row(key=5, text="toto", age=25, zip='20190', departement="2A", last_name="aha")
            ]
        )
        schema = StructType([
        StructField("departement", StringType(), True),
        StructField("count", LongType(), False)
        ])
        
        expected = spark.createDataFrame(
            [
                Row(departement="2B", count=1),
                Row(departement="2A", count=1),
                Row(departement="25", count=2)
            ], schema=schema
        )
                
        not_expected = spark.createDataFrame(
            [
                Row(departement="25", count=1),
                Row(departement="2B", count=1),
                Row(departement="2A", count=1)
            ], schema=schema
        )

        actual = get_count_per_departments(input, "count", "departement")
        
        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.schema, expected.schema)

        self.assertNotEqual(actual.collect(), not_expected.collect())