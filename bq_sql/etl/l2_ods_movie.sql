MERGE `decent-destiny-329402.demo_ods.ods_movie` ods
USING `decent-destiny-329402.demo_rds.rds_movie` rds
ON ods.movieid = rds.movieid and rds.dt="{{ params.dt }}"
WHEN MATCHED THEN
  UPDATE SET  ods.moviename=rds.moviename,
              ods.movietype=rds.moviename
WHEN NOT MATCHED THEN
  INSERT (movieid, moviename,movietype) VALUES(movieid, moviename,movietype);