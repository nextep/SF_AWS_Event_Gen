create or replace task EVENTS.EVENTS.ASSETS_UPDATE
	schedule='2 minute'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	as MERGE INTO ASSETS AS target
USING (
    SELECT source_ip, destination_ip, destination_hostname, source_hostname,  (SELECT MIN(timestamp) FROM staging_vw s WHERE s.source_ip = t.source_ip AND s.destination_ip = t.destination_ip) AS first_seen ,(SELECT MAX(timestamp) FROM staging_vw s WHERE s.source_ip = t.source_ip AND s.destination_ip = t.destination_ip) AS last_seen, data_source
    FROM staging_vw t
    GROUP BY source_ip, destination_ip, destination_hostname, source_hostname, data_source
    ) AS source
ON (target.source_ip = source.source_ip AND target.destination_ip = source.destination_ip AND target.source_hostname = source.source_hostname AND target.destination_hostname = source.destination_hostname AND target.data_source = source.data_source)
WHEN MATCHED THEN
  UPDATE SET target.added_on = current_timestamp,
             target.first_seen = GREATEST(target.first_seen, source.first_seen),
             target.last_seen = GREATEST(target.last_seen, source.last_seen)
WHEN NOT MATCHED THEN
  INSERT (destination_hostname, source_hostname, source_ip, destination_ip, first_seen, last_seen, data_source)
  VALUES (source.destination_hostname, source.source_hostname, source.source_ip, source.destination_ip, source.first_seen, source.last_seen, source.data_source);


alter task assets_update resume;
alter task assets_update suspend;
desc task assets_update ;

alter task assets_update modify as  MERGE INTO ASSETS AS target
USING (
    SELECT source_ip, destination_ip, destination_hostname, source_hostname,  (SELECT MIN(timestamp) FROM staging_vw s WHERE s.source_ip = t.source_ip AND s.destination_ip = t.destination_ip) AS first_seen ,(SELECT MAX(timestamp) FROM staging_vw s WHERE s.source_ip = t.source_ip AND s.destination_ip = t.destination_ip) AS last_seen, data_source
    FROM staging_vw t
    GROUP BY source_ip, destination_ip, destination_hostname, source_hostname, data_source
    ) AS source
ON (target.source_ip = source.source_ip AND target.destination_ip = source.destination_ip AND target.source_hostname = source.source_hostname AND target.destination_hostname = source.destination_hostname AND target.data_source = source.data_source)
WHEN MATCHED THEN
  UPDATE SET target.added_on = current_timestamp,
             target.first_seen = GREATEST(target.first_seen, source.first_seen),
             target.last_seen = GREATEST(target.last_seen, source.last_seen)
WHEN NOT MATCHED THEN
  INSERT (destination_hostname, source_hostname, source_ip, destination_ip, first_seen, last_seen, data_source)
  VALUES (source.destination_hostname, source.source_hostname, source.source_ip, source.destination_ip, source.first_seen, source.last_seen, source.data_source);;
