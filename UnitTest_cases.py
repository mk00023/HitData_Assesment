from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType
import unittest
import pytest

from Unittest_loacalfile import HitDataRevenueSummary

class TestMyClass(unittest.TestCase):

        
          
        
    def test_extract_revenue(self):
        # create test data
        spark = SparkSession.builder.appName("MyApp").getOrCreate()
        pathname ='local_path'
        df = spark.read.csv(pathname, header=True, inferSchema=True,sep="\t")  
        # call the function and expect a ValueError to be raised
        with self.assertRaises(ValueError):
            HitDataRevenueSummary().extract_revenue(df, "product_list")
            
        
if __name__ == "__main__":
    pytest.main()