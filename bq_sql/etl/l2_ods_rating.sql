MERGE `decent-destiny-329402.demo_ods.ods_rating` ods
USING `decent-destiny-329402.demo_rds.rds_rating` rds
ON ods.movieid = rds.movieid and ods.userid=rds.userid and rds.dt="{{ params.dt }}"
WHEN MATCHED THEN
  UPDATE SET  ods.rate=rds.rate,
              ods.times=rds.times
WHEN NOT MATCHED THEN
  INSERT (userid, movieid,rate,times) VALUES(userid, movieid,rate,times)