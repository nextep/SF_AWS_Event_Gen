Made this to play around with parsing securiy event data directly from an S3 bucket into SnowFlake with enrichment along the way. 

Used this script to generate test event files where you can control the values and use cases to do some performance testing.
-Gen2.py - Python script generates event files to test with and make it easy to add outlier events in the dataset. 

SQL files (Pads with SQL statements):
-AWS_SF_Integration.sql - S3 Storage integration with SnowFlake.
-Assets update.sql - Automation Task SQL pad
-assets_merge_S3_Stage.sql - SQL pad to merge targets with COPY INTO to assets
-external_table_materialized_view.sql - SQL Pad to create external materialized views with mappings to parse data in S3 files


