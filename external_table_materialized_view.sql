create external table ext_STAGING
(
Timestamp TIMESTAMP as ($1:timestamp::timestamp), Source_IP varchar as ($1:source_ip::string), Destination_IP varchar as ( $1:destination_ip::string), Source_hostname varchar as ($1:source_hostname::string), Destination_hostname varchar as ($1:destination_hostname::string), Data_Source varchar as ($1:data_source::string)
 ) 
      with location = @EVT_2_SF
    refresh_on_create = true 
    auto_refresh = true 
    file_format = JSON;
 
 Use SQS Option to configure an event on your S3 bucket.
https://docs.snowflake.com/en/user-guide/tables-external-s3

create materialized view staging_vw
    -- comment = '<comment>'
    as select TIMESTAMP,
	SOURCE_IP ,
	DESTINATION_IP ,
  source_hostname ,
  destination_hostname ,
  data_source from ext_staging;
