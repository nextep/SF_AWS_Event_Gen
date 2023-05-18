CREATE SEQUENCE Assets_seq;

CREATE TABLE Assets (
id INT default Assets_seq.NEXTVAL PRIMARY KEY,
  Added_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  source_hostname VARCHAR(255),
  source_ip VARCHAR(255),
  destination_hostname VARCHAR(255),
  destination_ip VARCHAR(255),
  first_seen TIMESTAMP,
  last_seen TIMESTAMP,
  data_source VARCHAR(255)
);


MERGE INTO ASSETS AS target
USING (
    SELECT source_ip, destination_ip, destination_hostname, source_hostname,  (SELECT MIN(timestamp) FROM staging s WHERE s.source_ip = t.source_ip AND s.destination_ip = t.destination_ip) AS first_seen ,(SELECT MAX(timestamp) FROM staging s WHERE s.source_ip = t.source_ip AND s.destination_ip = t.destination_ip) AS last_seen, data_source
    FROM staging t
    GROUP BY source_ip, destination_ip, destination_hostname, source_hostname,data_source
    ) AS source
ON (target.source_ip = source.source_ip AND target.destination_ip = source.destination_ip AND target.source_hostname = source.source_hostname AND target.destination_hostname = source.destination_hostname )
WHEN MATCHED THEN
  UPDATE SET target.added_on = current_timestamp,
             target.first_seen = GREATEST(target.first_seen, source.first_seen),
             target.last_seen = GREATEST(target.last_seen, source.last_seen)
WHEN NOT MATCHED THEN
  INSERT (destination_hostname, source_hostname, source_ip, destination_ip, first_seen, last_seen, data_source)
  VALUES (source.destination_hostname, source.source_hostname, source.source_ip, source.destination_ip, source.first_seen, source.last_seen, source.data_source);
