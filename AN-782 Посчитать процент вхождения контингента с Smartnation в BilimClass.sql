
-- AN-782 Посчитать процент вхождения контингента с Smartnation в BilimClass


-- выгрузка кол-во учеников Балабакша
SELECT oc.user_id, CONCAT(rsu.surname, ' ', rsu.firstname, ' ', rsu.lastname) AS fio
FROM dwh.obp_contingent oc
LEFT JOIN dwh.rms_system_user rsu ON rsu.user_id = oc.user_id;


-- выгрузка кол-во учеников Билимкласс
SELECT omc.user_id, omc.parallel, CONCAT(rsu.surname,' ', rsu.firstname,' ', rsu.lastname) AS fio
FROM dwh.omg_contingent omc
LEFT JOIN dwh.rms_system_user rsu ON rsu.user_id = omc.user_id
LEFT JOIN dwh.rms_schools s ON s.id = omc.organization_id
WHERE omc.parallel IN ('0','1') AND
      omc.deleted_at IS NULL AND
      s.deleted_at IS NULL AND
      s.api_version = 'v4';


-- выгрузка совпадение Билимкласс и Балабакша по ФИО
SELECT *
FROM (  SELECT omc.user_id, omc.parallel, CONCAT(rsu.surname,' ', rsu.firstname,' ', rsu.lastname) AS fio, rsu.iin
        FROM dwh.omg_contingent omc
        LEFT JOIN dwh.rms_system_user rsu ON rsu.user_id = omc.user_id
        LEFT JOIN dwh.rms_schools s ON s.id = omc.organization_id
        WHERE omc.parallel IN ('0','1') AND
              omc.deleted_at IS NULL AND
              s.deleted_at IS NULL AND
              s.api_version = 'v4') cs
INNER JOIN (SELECT oc.user_id, CONCAT(rsu.surname,' ', rsu.firstname,' ', rsu.lastname) AS fio
            FROM dwh.obp_contingent oc
            LEFT JOIN dwh.rms_system_user rsu ON rsu.user_id = oc.user_id) cnt ON cnt.fio = cs.fio




