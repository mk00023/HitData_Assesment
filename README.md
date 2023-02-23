# HitData_Assesment

This project contains artifacts around tab separated files inside an s3 bucket which contain "hit level data". 
A hit level record is a single "hit" from a visitor on the client's site. 

Based on the client's implementation, several variables can be set and sent to do Analytics for deeper analysis.

The main objective of the data pipeline is to calculate how much revenue is the client getting from external Search Engines, such as Google, Yahoo and MSN, and which keywords are performing the best based on revenue and write the output files to the s3 bucket.

### Getting Started
To get started with the project, follow these steps:

1. Clone the repository to your local machine
2. Install the necessary dependencies


### Prerequisites
To use this project, you'll need the following installed on your machine:

- pyspark
- numpy
- AWS account
- AWS Glue role and permissions set up for the account
- s3 Bucket : Replace with your bucketname
bucket_name = 'mybucketraw123'
a folder named 'processed' in s3 bucket : All processed files moved to this folder.
output_folder_prefix = 'processed/'

All the above packages are listed in the requirements.txt which can be used by the user for package installation using the command:
```
pip install -r requirements.txt
```

### Running the pipeline
1. Navigate to the AWS Glue console and create a new Glue job.
2. Select the Glue role you created in the prerequisites.
3. Configure the data source and destination for the job.
4. Upload the Glue job script (which is the code.py file in this repo).
5. Specify the S3 path for the script in the "Script path" field in the Glue job configuration.
6. Review and confirm the job configuration.
7. Start the Glue job and monitor the progress in the Glue console or logs(through CloudWatch).
