CREATE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::725788449614:role/SF_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://events169/To_Process')
  STORAGE_BLOCKED_LOCATIONS = ('s3://events169/Anomaly', 's3://events169/Processed');

  desc INTEGRATION s3_int;

  CREATE stage EVT_2_SF 
  URL = 's3://events169/To_Process'
  STORAGE_INTEGRATION = s3_int;

  
COPY INTO STAGING
FROM (
    select $1:timestamp::timestamp as Timestamp, $1:source_ip::string as Source_IP, $1:destination_ip::string as Destination_IP, $1:source_hostname::string as Source_hostname, $1:destination_hostname::string as Destination_hostname, $1:data_source::string as Data_Source from @EVT_2_SF (file_format => JSON)
);
