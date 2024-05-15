#--------Import required modules and packages------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum
from pyspark.sql.functions import *
from google.cloud import bigquery
from datetime import datetime

# Create Spark session
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-ingest-gcs-to-bigquery') \
  .getOrCreate()

# Define temporary GCS bucket for Dataproc to write it's process data
bucket='abc-ltd-datalake/tmp/dataproc'
spark.conf.set('temporaryGcsBucket', bucket)

###################### fetch ingestion file details from  job_detail table in bq###############################3

bq_client1 = bigquery.Client(project='your-project-name')
# job_detail_query = """select * from `your-project-name.ds_metadata_info.tbl_job_exec_detail_new`je 
# inner join `your-project-name.ds_metadata_info.file_metadata_info` fm 
# on SPLIT(fm.src_file_name, '.')[OFFSET(0)]=SPLIT(je.src_file_name, '-')[OFFSET(0)] 
# where je.process_step=1 and je.src_file_name not in (
# select src_file_name  from `your-project-name.ds_metadata_info.tbl_job_exec_detail_new` where process_step=2) 
# and fm.is_active=true order by je.src_file_date asc"""

job_detail_query = """select * from `your-project-name.ds_metadata_info.tbl_job_exec_detail_new`je 
inner join `your-project-name.ds_metadata_info.file_metadata_info` fm 
on SPLIT(fm.src_file_name, '.')[OFFSET(0)]=SPLIT(je.src_file_name, '-')[OFFSET(0)] 
where je.process_step=2 and fm.is_active=true and is_ingested is false and je.src_file_name in 
(select src_file_name  from `your-project-name.ds_metadata_info.tbl_job_exec_detail_new` 
where process_step=1 and is_ingested is true) 
order by je.src_file_date asc"""
get_job_detail = bq_client1.query(job_detail_query)
data = get_job_detail.result()
rows = list(data)
##################################################################################################################3


lst = list(rows)
if not lst:
    print("No files found to process")
    
    # Add pass here to continue the other steps
    pass
else:  
    for i in lst:
      d = dict(i)
      # print(d['src_file_path'])
      # print(d)

      df=spark.read.option("header",True).csv('gs://abc-ltd-datalake/'+d['src_file_path']+d['src_file_name'])

      req_df=df.withColumn("Ingestion_Date", current_timestamp()).withColumn("src_file_name", lit(d['src_file_name']))

      # Writing the data to BigQuery
      req_df.write.format('bigquery') \
        .option('table', d['stg_schema_name']+'.'+d['stg_table_name']) \
        .option('createDisposition','CREATE_IF_NEEDED') \
        .mode("append").save()

      row_count = req_df.count()
      print(f"{d['src_file_name']} - file ingested successfully with row count : {row_count} records.")

########################## Update the ingestion log into tbl_job_exec_detail table ########################################################3
      # status_query="""INSERT INTO your-project-name.ds_metadata_info.tbl_job_exec_detail_new
      # (rec_count,pipeline_step,PIPELINE_NAME,src_file_date,src_file_path,src_file_name,ingested_date,is_ingested,Process_Step) 
      # VALUES ("""+str(row_count)+""",'GCS to STG - Step 2/4','1-generic-ingest-sales-gcs-to-bq_stg',"""+"'"+str(d['src_file_date'])+ \
      #   "'"+','+"'"+d['src_file_path']+"'"+','+"'"+d['src_file_name']+"'"+','+"""CURRENT_DATETIME,True,2)"""

      status_query="""UPDATE your-project-name.ds_metadata_info.tbl_job_exec_detail_new set rec_count="""+str(row_count)+""",ingested_date=CURRENT_DATETIME,is_ingested=True,COMMENTS='Ingested' where is_ingested is false and process_step=2 and src_file_name='"""+d['src_file_name']+"'"
   
      # print(status_query)
      bq_client1 = bigquery.Client()
      query_job1 = bq_client1.query(status_query)
      query_job1.result()  #Waits for the query to finish
###################################################################################################################################

spark.stop()