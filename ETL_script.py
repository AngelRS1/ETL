#!/usr/bin/python3

import os
import datetime
import boto3
import logging
import configparser
import psycopg2
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

def initialize_Logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler('Jobs_ETL.log')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def parseConfig(logger):
    try:
        config = configparser.ConfigParser()
        config.read('Jobs_ETL.ini')
    except Exeption as inst:
        logger.info('Failed to parse configuration file. Error: ' + str(inst))
        quit()
    return config

def filesinfolders(client, bucket, prefix=''):
    paginator = client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        for prefix in result.get('Contents', []):
            yield prefix.get('Key')

def checkfiles(config,logger):
    S3Bucket = config['S3']['S3Bucket']
    S3FolderKey = config['S3']['S3FolderKey']
    S3AccesskeyID =config['S3']['S3AccesskeyID']
    S3Secretaccesskey = config['S3']['S3Secretaccesskey']

    try:
        print(str(datetime.datetime.now()) + ' - Looking for files to process... ')
        s3client = boto3.client(
            's3',
            aws_access_key_id=S3AccesskeyID,
            aws_secret_access_key=S3Secretaccesskey
        )	
        y = filesinfolders(s3client, S3Bucket, S3FolderKey)
    except Exeption as inst:
        logger.info('Failed to check files in S3. Error: ' + str(inst))
        quit()		
    file_list = []
    for file in y:
        filename = file[len(S3FolderKey):len(file)]
        file_list.append(filename)
    return file_list
		
def movefiletohistory(config,logger,filename):
    S3Bucket = config['S3']['S3Bucket']
    S3FolderKey = config['S3']['S3FolderKey']
    S3AccesskeyID =config['S3']['S3AccesskeyID']
    S3Secretaccesskey = config['S3']['S3Secretaccesskey']
    ProcessedFolderKey = config['S3']['ProcessedFolderKey']

    try:
        print(str(datetime.datetime.now()) + ' - Moving files to the already processed folder. File: '+filename)	
        session = boto3.Session(
            aws_access_key_id=S3AccesskeyID,
            aws_secret_access_key=S3Secretaccesskey,
        )

        s3 = session.resource('s3')
        s3_client = boto3.client('s3',
                                 aws_access_key_id=S3AccesskeyID,
                                 aws_secret_access_key=S3Secretaccesskey
                                )

        Filekey = S3FolderKey+'/'+filename
        copy_source = 	{
                         'Bucket': S3Bucket,
                         'Key': Filekey
                        }
        s3.meta.client.copy(copy_source, S3Bucket, ProcessedFolderKey+'/'+ filename)
        s3_client.delete_object(Bucket =S3Bucket,Key =Filekey)
        print(str(datetime.datetime.now()) + ' - File moved. ')	
    except Exeption as inst:
        logger.info('Failed to move file to the history. Error: ' + str(inst))
        quit()		

def redshift_connection(config):
    try:
        conn = psycopg2.connect(
            dbname=config['Redshift']['database'],
            user=config['Redshift']['user'],
            password=config['Redshift']['passwd'],
            port=config['Redshift']['port'],
            host=config['Redshift']['host'])
        return conn
    except Exception as ERROR:
        print("Failed connection to redshift with error: " + str(ERROR))
    return None

def executequeries(config,logger):
    SQL_Dim_Company_INSERT = """insert into analytics.Dim_Company (Company_name, Company_sector,Insert_date)
									select distinct Source.company,Source.sector,
									getdate()
									from
										(
										Select distinct company,sector
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Company Target
										On Target.Company_name = Source.company and Target.Company_sector = Source.sector
									Where Target.Company_name is null
                             """			
    SQL_Dim_Job_INSERT = """insert into analytics.Dim_Job (Id, Title,Insert_date)
									select distinct Source.id,Source.title,
									getdate()
									from
										(
										Select distinct id,title
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Job Target
										On Target.Id = Source.id and Target.Title = Source.title
									Where Target.Id is null
                             """	
    SQL_Dim_Status_INSERT = """insert into analytics.Dim_Status (Status, Insert_date)
									select distinct Source.adverts_status,
									getdate()
									from
										(
										Select distinct adverts_status
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Status Target
										On Target.Status = Source.adverts_status
									Where Target.Status is null
                             """	
    SQL_Dim_Location_INSERT = """insert into analytics.Dim_Location (City, Insert_date)
									select distinct Source.City,
									getdate()
									from
										(
										Select distinct City
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Location Target
										On Target.City = Source.City
									Where Target.City is null
                             """
    SQL_Dim_Advert_INSERT = """insert into analytics.Dim_Advert (Id, Apply_url,Insert_date)
									select distinct Source.adverts_id,Source.adverts_applyurl,
									getdate()
									from
										(
										Select distinct adverts_id,adverts_applyurl
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Advert Target
										On Target.Id = Source.adverts_id and Target.Apply_url = Source.adverts_applyurl
									Where Target.Id is null
                             """
    SQL_Dim_Applicant_INSERT = """insert into analytics.Dim_Applicant (First_name, Last_name,Insert_date)
									select distinct Source.applicants_firstname,Source.applicants_lastname,
									getdate()
									from
										(
										Select distinct applicants_firstname,applicants_lastname
										from analytics.tbl_stg_dump_file
										) Source
									LEFT JOIN analytics.Dim_Applicant Target
										On Target.First_name = Source.applicants_firstname and Target.Last_name = Source.applicants_lastname
									Where Target.First_name is null
                             """
    SQL_Fact_job_advert_INSERT = """Insert into analytics.Fact_job_advert(Dim_advert_id,Dim_compay_id,Dim_job_id,Dim_status_id,Dim_location_id,Publication_date_id,Active_days,Insert_date)
								  SELECT distinct
										AD.dim_advert_id,
										CMP.dim_company_id,
										J.dim_job_id,
										S.dim_status_id,
										L.dim_location_id,
										D.dim_date_id,
										Source.adverts_activedays,
										getdate()
								  from analytics.tbl_stg_dump_file source
								  LEFT JOIN analytics.Dim_advert AD
									on AD.Id = Source.adverts_id and AD.Apply_url = Source.adverts_applyurl
								  LEFT JOIN analytics.Dim_Company CMP
									on CMP.Company_name = source.company and CMP.Company_sector = source.sector
								  LEFT JOIN analytics.Dim_Job J
									on J.Id = source.id and J.Title = source.title
								  LEFT JOIN analytics.Dim_Status S
									on S.Status = source.adverts_status
								  LEFT JOIN analytics.Dim_Location L
									on L.City = source.city
								  LEFT JOIN analytics.dim_Date D
									on D.Fulldate = (timestamp 'epoch' + source.adverts_publicationdatetime * interval '1 second')::date
                             """
    SQL_Fact_job_applicant_INSERT = """Insert into analytics.Fact_job_applicant(Dim_compay_id,Dim_job_id,Dim_location_id,Application_date_id,Dim_applicant_id,Age,Insert_date)
								  SELECT distinct
										CMP.dim_company_id,
										J.dim_job_id,
										L.dim_location_id,
										D.dim_date_id,
										AD.dim_applicant_id,
										Source.applicants_age,
										getdate()
								  from analytics.tbl_stg_dump_file source
								  LEFT JOIN analytics.Dim_Applicant AD
									on AD.First_name = Source.applicants_firstname and AD.Last_name = Source.applicants_lastname
								  LEFT JOIN analytics.Dim_Company CMP
									on CMP.Company_name = source.company and CMP.Company_sector = source.sector
								  LEFT JOIN analytics.Dim_Job J
									on J.Id = source.id and J.Title = source.title
								  LEFT JOIN analytics.Dim_Status S
									on S.Status = source.adverts_status
								  LEFT JOIN analytics.Dim_Location L
									on L.City = source.city
								  LEFT JOIN analytics.dim_Date D
									on D.Fulldate = (timestamp 'epoch' + source.applicants_applicationdate * interval '1 second')::date
                             """
    try:
        print(str(datetime.datetime.now()) + ' - Establishing connection to Redshift...')
        conn = redshift_connection(config)				
        rscursor  = conn.cursor()
        print(str(datetime.datetime.now()) + ' - Execute queries to fill dim tables')
        rscursor.execute(SQL_Dim_Company_INSERT)
        conn.commit()
        rscursor.execute(SQL_Dim_Job_INSERT)
        conn.commit()
        rscursor.execute(SQL_Dim_Status_INSERT)
        conn.commit()
        rscursor.execute(SQL_Dim_Location_INSERT)
        conn.commit()
        rscursor.execute(SQL_Dim_Advert_INSERT)
        conn.commit()
        rscursor.execute(SQL_Dim_Applicant_INSERT)
        conn.commit()
        print(str(datetime.datetime.now()) + ' - Execute queries to fill fact tables')
        rscursor.execute(SQL_Fact_job_advert_INSERT)
        conn.commit()
        rscursor.execute(SQL_Fact_job_applicant_INSERT)
        conn.commit()
        rscursor.close()
	    conn.close()
    except Exeption as inst:
        logger.info('Failed to execute the queries. Error: ' + str(inst))
        quit()		

def work_json_file(config,logger,filename):
    dbname=config['Redshift']['database']
    dbuser=config['Redshift']['user']
    dbpassword=config['Redshift']['passwd']
    port=config['Redshift']['port']
    host=config['Redshift']['host']
    dump_table = ['Redshift']['dump_table']

    S3Bucket = config['S3']['S3Bucket'] #Format should be like: BuckeName
    S3FolderKey = config['S3']['S3FolderKey'] #Format should be like: subfolder/subfolder
    S3AccesskeyID =config['S3']['S3AccesskeyID']
    S3Secretaccesskey = config['S3']['S3Secretaccesskey']
    
    try:
        print(str(datetime.datetime.now()) + ' - Create spark context and load json from s3...')
        SC = SparkContext()
        sqlCTx = SQLContext(SC)
        
        JsonPath = "s3n://"+S3Bucket+"/"+S3FolderKey+"/"+filename
		
        hadoop_conf=SC._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        hadoop_conf.set("fs.s3n.awsAccessKeyId", S3AccesskeyID)
        hadoop_conf.set("fs.s3n.awsSecretAccessKey", S3Secretaccesskey)

        Data = sqlCTx.read.json(JsonPath, multiLine = "true")

        print(str(datetime.datetime.now()) + ' - Transform json structure to a plain table...')
        DF_Tramsform1 = Data.select("id","title","company","sector","city","adverts.activeDays","adverts.applyUrl","adverts.id","adverts.publicationDateTime","adverts.status",explode("applicants").alias("applicants")) \
            .withColumnRenamed("adverts.activeDays","adverts_activedays") \
            .withColumnRenamed("adverts.applyUrl","adverts_applyurl") \
            .withColumnRenamed("adverts.id","adverts_id") \
            .withColumnRenamed("adverts.publicationDateTime","adverts_publicationdatetime") \
            .withColumnRenamed("adverts.status","adverts_status")

        DF_Tramsform2 = DF_Tramsform1.select("id","title","company","sector","city","adverts_activedays","adverts_applyurl","adverts_id","adverts_publicationdatetime","adverts_status","applicants.age","applicants.applicationDate","applicants.firstName","applicants.lastName") \
            .withColumnRenamed("age","applicants_age") \
            .withColumnRenamed("applicationDate","applicants_applicationdate") \
            .withColumnRenamed("firstName","applicants_firstname") \
            .withColumnRenamed("lastName","applicants_lastname") 

        ServerConnection = 'jdbc:redshift://'+host+':'+port+'/'+dbname
		
        print(str(datetime.datetime.now()) + ' - Load data to redshift...')
        DF_Tramsform2.write.format('jdbc').options(
            url=ServerConnection,
            driver='com.amazon.redshift.jdbc42.Driver',
            dbtable=dump_table,
            user=dbuser,
            password=dbpassword).mode('overwrite').save()
        except Exeption as inst:
            logger.info('Failed transform and load json file to redshift. Error: ' + str(inst))	
			return False
    return True

	
def main():
    # We initialize the logging component to output the error log
    logger = initialize_Logger()
    # We try to read the configuration options
    config = parseConfig(logger)
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
    file_list = checkfiles(config,logger)
    for file in file_list:
        if work_json_file(config,logger,file):
            executequeries(config,logger)
            movefiletohistory(config,logger,file)
    print(str(datetime.datetime.now()) + ' - Process finish.')
            

if __name__ == "__main__":
    main()
