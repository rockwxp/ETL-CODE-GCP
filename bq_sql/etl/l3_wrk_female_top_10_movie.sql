DROP TABLE IF EXISTS `{{ params.project }}.demo_wrk.wrk_femle_top_10_movie`;
CREATE TABLE `{{ params.project }}.demo_wrk.wrk_femle_top_10_movie`
    AS
SELECT
  a.movieid AS movieid,
  a.rate AS rate
FROM
  `{{ params.project }}.demo_ods.ods_rating` a
WHERE
  a.userid=(
  SELECT userid FROM (
    SELECT  a.userid, COUNT(a.userid) AS total
    FROM `{{ params.project }}.demo_ods.ods_rating` a
    JOIN `{{ params.project }}.demo_ods.ods_user` b
    ON  a.userid = b.userid WHERE  b.sex="F"
    GROUP BY  a.userid
    ORDER BY total DESC
    LIMIT 1 )
    )
ORDER BY
  rate DESC
LIMIT
  10;