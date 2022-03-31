DROP TABLE IF EXISTS `{{ params.project }}.demo_dm.dm_femle_top_10_movie`;
create table `{{ params.project }}.demo_dm.dm_femle_top_10_movie` as
SELECT
  b.movieid AS movieid,
  c.moviename AS moviename,
  AVG(b.rate) AS avgrate
FROM
  `{{ params.project }}.demo_wrk.wrk_femle_top_10_movie` a
JOIN
  `{{ params.project }}.demo_ods.ods_rating` b
ON
  a.movieid=b.movieid
JOIN
  `{{ params.project }}.demo_ods.ods_movie` c
ON
  b.movieid=c.movieid
GROUP BY
  b.movieid,
  c.moviename;



