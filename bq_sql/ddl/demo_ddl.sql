--rds
CREATE EXTERNAL TABLE `decent-destiny-329402.demo_rds.rds_user`
(
     userid INT64,
     sex string,
     age int,
     occupation string,
     zipcode string
)
WITH PARTITION COLUMNS
OPTIONS(
    uris=["gs://solana-rds-bucket/demo_rds/rds_user/*"],
    format="CSV",
    hive_partition_uri_prefix="gs://solana-rds-bucket/demo_rds/rds_user"
);
CREATE EXTERNAL TABLE `decent-destiny-329402.demo_rds.rds_movie`
(
     movieid bigint,
     moviename string,
     movietype string
)
WITH PARTITION COLUMNS
OPTIONS(
    uris=["gs://solana-rds-bucket/demo_rds/rds_movie/*"],
    format="CSV",
    hive_partition_uri_prefix="gs://solana-rds-bucket/demo_rds/rds_movie"
);
CREATE EXTERNAL TABLE `decent-destiny-329402.demo_rds.rds_rating`
(
     userid bigint,
     movieid bigint,
     rate DECIMAL,
     times string
)
WITH PARTITION COLUMNS
OPTIONS(
    uris=["gs://solana-rds-bucket/demo_rds/rds_rating/*"],
    format="CSV",
    hive_partition_uri_prefix="gs://solana-rds-bucket/demo_rds/rds_rating"
);
-- ods
CREATE TABLE `decent-destiny-329402.demo_ods.ods_user`
(
     userid INT64,
     sex string,
     age int,
     occupation string,
     zipcode string
)
CLUSTER BY userid;

CREATE TABLE `decent-destiny-329402.demo_ods.ods_movie`
(
     movieid bigint,
     moviename string,
     movietype string
)
CLUSTER BY movieid;

CREATE TABLE `decent-destiny-329402.demo_ods.ods_rating`
(
     userid bigint,
     movieid bigint,
     rate DECIMAL,
     times string
)
CLUSTER BY userid,movieid;