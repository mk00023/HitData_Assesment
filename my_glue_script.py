#import libraries

import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import split, explode, sum
from pyspark.sql.functions import lower
import urllib
import datetime
import os
import warnings
warnings.filterwarnings("ignore")
import boto3
import pandas as pd


# create an S3 client
s3 = boto3.client('s3')


class HitDataRevenueSummary:
    
    """
    A class that processes the Hit level data in the s3 file for the client and calculates the Revenue generated from external search engines based on popular keywords on a daily level.

    """
    
    
    def __init__ (self):
        """ """        
        

################   

    def extract_valid_ips(self,source_dataframe):
        
        """
        Extracts distinct ip adresses (ips) from the data that have a purchase event.

        Args:
            source_dataframe: pyspark dataframe with hit level data from s3 file.

        Returns:
            valid_ips_df: pyspark dataframe with ips that have a purchase event.
        """
        
        #create temporary view to be able to query for valid ips
        source_dataframe.createOrReplaceTempView("hit_data_table")

        #extract ips where there is a purchase event
        valid_ips_df = spark.sql("select distinct ip from hit_data_table where event_list =1 ")

        return valid_ips_df

################   
        
    def extract_domain_client_url(self,df, pattern, input_col, output_col):
              
        """
        Extracts domain/client url from the data.

        Args:
            df: pyspark dataframe with hit level data for ips that have a purchase event.
            pattern: regex pattern used to extract domain/client url from input_col
            input_col: the column used to extract the domain/client url using regex pattern.
            output_col: the name of the column to store the extracted domain/client url after regex pattern matching.

        Returns:
            df: pyspark dataframe with ip, input_col and extracted output_col(domain/client url)
        """
        
        #converting the input_col to lower case
        df = df.withColumn(input_col, lower(df[input_col]))

        # Apply regex on input_col and add the output_col(domain/client url) to the DataFrame
        df = df.withColumn(output_col, regexp_extract(df[input_col], pattern, 1))
        
        return df
    
#################       

    def extract_search_keyword_from_url(self,df, regex, url_col, keyword_col):
        
        """
        Extracts keyword from the data.

        Args:
            df: pyspark dataframe with hit level data for ips that have a purchase event.
            regex: regex pattern used to extract keyword from url_col
            url_col: the column used to extract the keyword using regex pattern.
            keyword_col: the name of the column to store the extracted keyword after regex pattern matching.

        Returns:
            df: pyspark dataframe with ip, url_col and extracted keyword
        """
        #converting the url_col to lower case
        df = df.withColumn(url_col, lower(df[url_col]))

        # Apply regex on url_col and add the extracted search keyword as a new column to the DataFrame
        df = df.withColumn(keyword_col, regexp_extract(df[url_col], regex, 1))
        
        return df
    
#################

    def extract_revenue(self,df,input_col):
 
        
        """ Extracts revenue using string operations from the product_list column
        
        Args:
            df: pyspark dataframe with ip and product_list information from data for ips that have a purchase event.
            input_col: column used to extract revenue from


        Returns:
            df: pyspark dataframe with ip and its corresponding Revenue
        """
        try:

            # split the "product_list" column on ","
            df = df.withColumn(input_col, split(col(input_col), ","))


            # split each element on ";"
            df = df.select("ip",explode(input_col).alias("product_info"))
            df = df.withColumn("product_info", split(col("product_info"), ";"))


            # select the fourth element from the array and cast it to float
            df = df.select("ip",col("product_info")[3].cast("float").alias("price"))
            
            if df.filter(col("price") < 0).count() > 0:
                raise ValueError("The revenue is negative...")

            # if multiple elements are there, sum them up 
            df = df.groupBy("ip").agg(sum("price").alias("revenue"))
            df = df.withColumn("revenue", col("revenue").cast("float"))

            # get the result
            return df
        
        except Exception as e:
            raise e

#################

    def generate_op_file(self,source_dataframe):
        
        """ Executes the full data processing pipeline to generate the final output file with Revenue information.
        
        Args:
            source_dataframe: pyspark df with s3 file data


        Returns:
            final_df: pyspark df with Search_Engine_Domain,Search_Keyword and Revenue information
        """
        
        
        # get all valid ips where there is a purchase event
        valid_ips_df = self.extract_valid_ips(source_dataframe)
        
        
        #create temporary view to be able to query for valid ips
        valid_ips_df.createOrReplaceTempView("valid_ip_table")
        
        
        #create temporary view to be able to query for all the hit data from the source file
        source_dataframe.createOrReplaceTempView("hit_data_table")
        

        #filter data for ip, page_url and referrer for valid ips
        filtered_valid_ip_df = spark.sql("select  a.ip ,a.page_url,a.referrer from hit_data_table a inner join valid_ip_table b on a.ip=b.ip order by ip,date_time")
        
        #apply functions to extarct domain, client url and keywords        
        domain_pattern=r'www\.([^/]+)'
        client_url_pattern=r'www\.([^/]+)'
        keyword_regex=r"q=([^&]*)"

        filtered_valid_ip_df = self.extract_domain_client_url(filtered_valid_ip_df, domain_pattern, "referrer", "domain_name")
        filtered_valid_ip_df = self.extract_domain_client_url(filtered_valid_ip_df, client_url_pattern, "page_url", "original_url")
        filtered_valid_ip_df = self.extract_search_keyword_from_url(filtered_valid_ip_df, keyword_regex, "referrer", "keyword")
        
        
        
        domain_keyword_df = filtered_valid_ip_df.where(filtered_valid_ip_df.domain_name != filtered_valid_ip_df.original_url)[['ip','domain_name','keyword']]
        
        
        #create temporary view 
        domain_keyword_df.createOrReplaceTempView("domain_keyword_table")
        
        
        product_list_valid_ip_df = spark.sql("select  a.ip, a.product_list from hit_data_table a inner join domain_keyword_table b on a.ip=b.ip where a.event_list=1 ")
        
        
        #revenue extraction
        product_list_valid_ip_df=self.extract_revenue(product_list_valid_ip_df,"product_list")
        
        
        #create temporary view 
        product_list_valid_ip_df.createOrReplaceTempView("revenue_table")
        
        
        # query on the temporary tables to get the output data in the desired format
        final_df=spark.sql("select  domain_name as Search_Engine_Domain ,keyword as Search_Keyword,sum(revenue) as Revenue from revenue_table a inner join domain_keyword_table b on a.ip=b.ip  group by 1,2 order by 3 desc")
     
        
        #create desired naming convention for the tsv file
        today = datetime.date.today()

        #pathname=os.getcwd()
        pathname ='s3://eshopzillas3/'
        filename =pathname+ str(today) + '_SearchKeywordPerformance.tab'
        print('Output file name is ',filename)

        # write the output to a tab separated file
        #final_df.write.format("csv").option("delimiter", "\t").option("header", "true").mode("overwrite").save(filename)
        final_df.toPandas().to_csv(filename, sep='\t', index=False ,header =True)
        
        #return final_df

#################    
    
    
#create instance of class
processing_functions = HitDataRevenueSummary()

# print('hello world')





# create a SparkSession object
spark = SparkSession.builder.appName("ReadS3Files").getOrCreate()
      
      
# specify the bucket name and folder prefix for the TSV files
bucket_name = 'mybucketraw123'

# specify the bucket name and folder prefix for the output TSV files
output_bucket_name = 'eshopzillas3'
output_folder_prefix = 'processed/'

# list all objects in the bucket with the specified prefix
objects = s3.list_objects(Bucket=bucket_name)


# retrieve the S3 keys for the tsv files(In case there are more than one files)
keys = [obj['Key'] for obj in objects['Contents'] if obj['Key'].endswith('.tsv')]
print('File to process: ', keys)

# specify the S3 paths to the tsv files
s3_paths = [f's3://{bucket_name}/{key}' for key in keys]
print('Before applying escape chracters for [ ] in file', s3_paths)

# Loop through the list of file names and replace square brackets with escaped square brackets
s3_paths = [f's3://{bucket_name}/'+ file_name.replace("[", "\[").replace("]", "\]") for file_name in keys]


print('After applying escape chracter', s3_paths)
# read all tsv files in the specified S3 paths into a Spark dataframe
df = spark.read.csv(s3_paths, header=True, inferSchema=True,sep="\t")

# print the dataframe schema 
df.printSchema()


#Process the data frame
#executing the pipeline code
processing_functions.generate_op_file(df)




#  copy the input TSV files to the processed folder
for file  in keys:
    if file.endswith('.tsv'):
        old_key = file
        new_key = output_folder_prefix + old_key
        print('file to be moved is: ', bucket_name,old_key)
        print('new location is: ', output_bucket_name,new_key)
        
        #copy files to output bucket and remove them from source bucket post copy
        s3.copy_object(Bucket=output_bucket_name, CopySource={'Bucket': bucket_name, 'Key': old_key}, Key=new_key)
        s3.delete_object(Bucket=bucket_name, Key=old_key)
    
        