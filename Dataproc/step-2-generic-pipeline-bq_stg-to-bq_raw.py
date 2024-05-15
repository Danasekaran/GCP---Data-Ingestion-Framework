from google.cloud import bigquery

# Create a new Google BigQuery client using Google Cloud Platform project
bq_client = bigquery.Client()
job_config = bigquery.QueryJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND,)


###################### fetch ingestion file details from  job_detail table in bq###############################3
#
# process one file data at a time. So we can maintain log status on each file basis
# but, we can ingest all the files data at a time also.
#

bq_client1 = bigquery.Client(project='your-project-name')
# job_detail_query = """select * from `your-project-name.ds_metadata_info.tbl_job_exec_detail_new`je 
#                     inner join `your-project-name.ds_metadata_info.file_metadata_info` fm 
#                     on SPLIT(fm.src_file_name, '.')[OFFSET(0)]=SPLIT(je.src_file_name, '-')[OFFSET(0)] 
#                     where je.process_step=2 and je.src_file_name not in (
#                     select src_file_name  from `your-project-name.ds_metadata_info.tbl_job_exec_detail_new` where process_step=3) 
#                     and fm.is_active=true order by je.src_file_date asc"""
job_detail_query = """select * from `your-project-name.ds_metadata_info.tbl_job_exec_detail_new`je 
                        inner join `your-project-name.ds_metadata_info.file_metadata_info` fm 
                        on SPLIT(fm.src_file_name, '.')[OFFSET(0)]=SPLIT(je.src_file_name, '-')[OFFSET(0)] 
                        where je.process_step=3 and fm.is_active=true and is_ingested is false and je.src_file_name in 
                        (select src_file_name  from `your-project-name.ds_metadata_info.tbl_job_exec_detail_new` 
                        where process_step=2 and is_ingested is true) 
                        order by je.src_file_date asc"""

# print(job_detail_query)
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
      #  print(d['src_file_path'])
      #  print(d)       
       job_config.destination = f"{bq_client.project}.{d['raw_schema_name']}.{d['raw_table_name']}"
      #  print(job_config.destination)
       query = """SELECT """ +d['exp_columns']+ """,current_timestamp() as Ingestion_Date,'"""+d['src_file_name']+"""' as src_file_name 
            FROM """ +d['stg_schema_name']+'.'+d['stg_table_name'] +""" where src_file_name=""" + "'"+d['src_file_name']+"'"

       query_job = bq_client.query(query, job_config=job_config)
       data=query_job.result()  # Waits for the query to finish
       print(f"{d['src_file_name']} - file ingested successfully with row count : {str(query_job._query_results.total_rows)} records.")

       row_count=query_job._query_results.total_rows

########################## Update the ingestion log into tbl_job_exec_detail table ########################################################3

       bq_client1 = bigquery.Client(project='your-project-name')
    #    status_query="""INSERT INTO your-project-name.ds_metadata_info.tbl_job_exec_detail_new
    #     (rec_count,pipeline_step,PIPELINE_NAME,src_file_date,src_file_path,src_file_name,ingested_date,is_ingested,Process_Step) 
    #     VALUES ("""+str(row_count)+""",'STG to RAW - Step 3/4','2-generic-move-sales-bq_stg-to-bq_raw',"""+"'"+str(d['src_file_date'])+ \
    #         "'"+','+"'"+d['src_file_path']+"'"+','+"'"+d['src_file_name']+"'"+','+"""CURRENT_DATETIME,True,3)"""

       status_query="""UPDATE your-project-name.ds_metadata_info.tbl_job_exec_detail_new set rec_count="""+str(row_count)+""",ingested_date=CURRENT_DATETIME,is_ingested=True,COMMENTS='Ingested' where is_ingested is false and process_step=3 and src_file_name='"""+d['src_file_name']+"'"


      #  print(status_query)
       query_job1 = bq_client1.query(status_query)
       query_job1.result()  #Waits for the query to finish

###################################################################################################################################