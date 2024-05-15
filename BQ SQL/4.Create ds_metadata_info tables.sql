-- create ds_metadata_info table 
CREATE TABLE ds_metadata_info.tbl_job_exec_detail_new
(
  pipeline_step STRING,
  PIPELINE_NAME STRING,
  src_file_date DATETIME,
  src_file_path STRING,
  src_file_name STRING,
  is_ingested BOOL DEFAULT False,
  ingested_date DATETIME,
  rec_count INT64 DEFAULT NULL,
  comments STRING,
  Process_Step INT64
);

CREATE TABLE ds_metadata_info.file_metadata_info
(
  Srno INT64 NOT NULL,
  src_file_name STRING NOT NULL,
  exp_columns STRING NOT NULL,
  is_active BOOL NOT NULL,
  stg_schema_name STRING,
  stg_table_name STRING,
  raw_schema_name STRING,
  raw_table_name STRING,
  curated_schema_name STRING,
  curated_table_name STRING,
  stg_query STRING,
  raw_to_curated_qry STRING
);