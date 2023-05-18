CREATE TABLE Assets (
id INT default Assets_seq.NEXTVAL PRIMARY KEY,
  Added_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  source_ip VARCHAR(255),
  destination_ip VARCHAR(255),
  first_seen TIMESTAMP,
  last_seen TIMESTAMP
);


MERGE INTO ASSETS AS target
USING (
    SELECT source_ip, destination_ip, (SELECT MIN(timestamp) FROM staging s WHERE s.source_ip = t.source_ip AND s.destination_ip = t.destination_ip) AS first_seen ,(SELECT MAX(timestamp) FROM staging s WHERE s.source_ip = t.source_ip AND s.destination_ip = t.destination_ip) AS last_seen
    FROM staging t
    GROUP BY source_ip, destination_ip
    ) AS source
ON (target.source_ip = source.source_ip AND target.destination_ip = source.destination_ip )
WHEN MATCHED THEN
  UPDATE SET target.added_on = current_timestamp,
             target.first_seen = GREATEST(target.first_seen, source.first_seen),
             target.last_seen = GREATEST(target.last_seen, source.last_seen)
WHEN NOT MATCHED THEN
  INSERT (source_ip, destination_ip, first_seen, last_seen)
  VALUES (source.source_ip, source.destination_ip, source.first_seen, source.last_seen);
