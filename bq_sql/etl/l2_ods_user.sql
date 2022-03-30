UPDATE `decent-destiny-329402.demo_ods.ods_user` ods
SET ods.sex = rds.sex,
    ods.age = rds.age,
    ods.occupation = rds.occupation,
    ods.zipcode = rds.zipcode
FROM `decent-destiny-329402.demo_rds.rds_user` rds
WHERE ods.userid = rds.userid and rds.dt="{{ params.dt }}";


INSERT `decent-destiny-329402.demo_ods.ods_user`  (userid, sex,age,occupation,zipcode)
SELECT userid, sex,age,occupation,zipcode
FROM `decent-destiny-329402.demo_rds.rds_user`
WHERE NOT userid IN (SELECT userid FROM `decent-destiny-329402.demo_ods.ods_user`) and dt="{{ params.dt }}";