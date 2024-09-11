-- AN-830 Monitoring SOR SOCH

-- Мониторинг СОР/СОЧ t2
-- CREATE MATERIALIZED VIEW dwh_bi.monitor_sor_soch_t2 AS
SELECT row_number() over ()                                         AS t2_id,
       eps.uuid                                                     AS eps_uuid,
       os.school_id                                                 AS school_id,
       os.group_id                                                  AS group_id,
       og.name                                                      AS parallel,
       og.letter                                                    AS letter,
       ogs.uuid                                                     AS subgroup_uuid,
       ogs.name                                                     AS subgroup,
       CASE WHEN og.parent_id IS NOT NULL THEN 1 ELSE 2 END         AS program_id,
       CASE WHEN og.parent_id IS NOT NULL THEN 'ИПО' ELSE 'ОПО' END as program_type,
       ts.id                                                        AS subject_id,
       ts.ru_name                                                   AS subject_name,
       eps.period_type                                              AS period_type,
       eyp.period                                                   AS period,
       sum(case when os.type = 'sor' then 1 else 0 end)             AS count_sor,
       sum(case when os.type = 'soch' then 1 else 0 end)            AS count_soch

FROM dwh.oms_schedules os

         LEFT JOIN dwh.oms_education_program_subjects eps ON eps.uuid = os.edu_program_subject_uuid
         LEFT JOIN dwh.omp_group_subgroups ogs ON ogs.uuid = os.subgroup_uuid
         LEFT JOIN dwh.omp_groups og ON og.id = os.group_id
         LEFT JOIN dwh.omg_taught_subjects ts ON ts.id = eps.subject_id
         INNER JOIN dwh.omg_education_year_periods eyp ON eyp.school_id = os.school_id

WHERE os.type IN ('sor', 'soch')
  AND eyp.period_type = 'quarter'
  AND os.date BETWEEN eyp.period_start AND eyp.period_end
GROUP BY eps.uuid, os.school_id, os.group_id, og.name, og.letter, ogs.uuid, ogs.name,
         CASE WHEN og.parent_id IS NOT NULL THEN 1 ELSE 2 END,
         CASE WHEN og.parent_id IS NOT NULL THEN 'ИПО' ELSE 'ОПО' END,
         ts.id, ts.ru_name, eps.period_type, eyp.period
;


-- Мониторинг СОР/СОЧ t1
-- CREATE MATERIALIZED VIEW dwh_bi.monitor_sor_soch_t1 AS
SELECT cs.id            AS t1_id,
       cs.eps_uuid      AS eps_uuid,
       cs.school_id     AS school_id,
       cs.group_id      AS group_id,
       cs.parallel      AS parallel,
       cs.letter        AS letter,
       cs.subgroup_uuid AS subgroup_uuid,
       cs.subgroup      AS subgroup,
       cs.program_id    AS program_id,
       cs.program_type  as program_type,
       cs.subject_id    AS subject_id,
       cs.subject_name  AS subject_name,
       cs.period_type   AS period_type,
       cs.period        AS period,
       cs.period_end    AS period_end,
       cs.type          AS type,
       cs.date          AS date,
       ms.t2_id         AS t2_id
FROM (SELECT row_number() over ()                                         AS id,
             eps.uuid                                                     AS eps_uuid,
             os.school_id                                                 AS school_id,
             os.group_id                                                  AS group_id,
             og.name                                                      AS parallel,
             og.letter                                                    AS letter,
             ogs.uuid                                                     AS subgroup_uuid,
             ogs.name                                                     AS subgroup,
             CASE WHEN og.parent_id IS NOT NULL THEN 1 ELSE 2 END         AS program_id,
             CASE WHEN og.parent_id IS NOT NULL THEN 'ИПО' ELSE 'ОПО' END AS program_type,
             ts.id                                                        AS subject_id,
             ts.ru_name                                                   AS subject_name,
             eps.period_type                                              AS period_type,
             eyp.period                                                   AS period,
             os.type                                                      AS type,
             os.date                                                      AS date,
             eyp.period_end                                               AS period_end

      FROM dwh.oms_schedules os

               LEFT JOIN dwh.oms_education_program_subjects eps ON eps.uuid = os.edu_program_subject_uuid
               LEFT JOIN dwh.omp_group_subgroups ogs ON ogs.uuid = os.subgroup_uuid
               LEFT JOIN dwh.omp_groups og ON og.id = os.group_id
               LEFT JOIN dwh.omg_taught_subjects ts ON ts.id = eps.subject_id
               INNER JOIN dwh.omg_education_year_periods eyp ON eyp.school_id = os.school_id

      WHERE os.type IN ('sor', 'soch')
        AND eyp.period_type = 'quarter'
        AND os.date BETWEEN eyp.period_start AND eyp.period_end
      GROUP BY eps.uuid, os.school_id, os.group_id, og.name, og.letter, ogs.uuid, ogs.name, eyp.period, ts.id,
               ts.ru_name, eps.period_type,
               os.date, eyp.period_end, os.type,
               CASE WHEN og.parent_id IS NOT NULL THEN 'ИПО' ELSE 'ОПО' END,
               CASE WHEN og.parent_id IS NOT NULL THEN 1 ELSE 2 END) cs

         LEFT JOIN dwh_bi.monitor_sor_soch_t2 ms
                   ON ms.school_id = cs.school_id AND
                      ms.group_id = cs.group_id AND
                      ms.subject_id = cs.subject_id AND
                      ms.period_type = cs.period_type AND
                      ms.period = cs.period AND
                      ms.subgroup = cs.subgroup

WHERE cs.subgroup IS NOT NULL
UNION
SELECT cs.id            AS t1_id,
       cs.eps_uuid      AS eps_uuid,
       cs.school_id     AS school_id,
       cs.group_id      AS group_id,
       cs.parallel      AS parallel,
       cs.letter        AS letter,
       cs.subgroup_uuid AS subgroup_uuid,
       cs.subgroup      AS subgroup,
       cs.program_id    AS program_id,
       cs.program_type  as program_type,
       cs.subject_id    AS subject_id,
       cs.subject_name  AS subject_name,
       cs.period_type   AS period_type,
       cs.period        AS period,
       cs.period_end    AS period_end,
       cs.type          AS type,
       cs.date          AS date,
       ms.t2_id         AS t2_id
FROM (SELECT row_number() over ()                                         AS id,
             eps.uuid                                                     AS eps_uuid,
             os.school_id                                                 AS school_id,
             os.group_id                                                  AS group_id,
             og.name                                                      AS parallel,
             og.letter                                                    AS letter,
             ogs.uuid                                                     AS subgroup_uuid,
             ogs.name                                                     AS subgroup,
             CASE WHEN og.parent_id IS NOT NULL THEN 1 ELSE 2 END         AS program_id,
             CASE WHEN og.parent_id IS NOT NULL THEN 'ИПО' ELSE 'ОПО' END AS program_type,
             ts.id                                                        AS subject_id,
             ts.ru_name                                                   AS subject_name,
             eps.period_type                                              AS period_type,
             eyp.period                                                   AS period,
             os.type                                                      AS type,
             os.date                                                      AS date,
             eyp.period_end                                               AS period_end

      FROM dwh.oms_schedules os

               LEFT JOIN dwh.oms_education_program_subjects eps ON eps.uuid = os.edu_program_subject_uuid
               LEFT JOIN dwh.omp_group_subgroups ogs ON ogs.uuid = os.subgroup_uuid
               LEFT JOIN dwh.omp_groups og ON og.id = os.group_id
               LEFT JOIN dwh.omg_taught_subjects ts ON ts.id = eps.subject_id
               INNER JOIN dwh.omg_education_year_periods eyp ON eyp.school_id = os.school_id

      WHERE os.type IN ('sor', 'soch')
        AND eyp.period_type = 'quarter'
        AND os.date BETWEEN eyp.period_start AND eyp.period_end
      GROUP BY eps.uuid, os.school_id, os.group_id, og.name, og.letter, ogs.uuid, ogs.name, eyp.period, ts.id,
               ts.ru_name, eps.period_type,
               os.date, eyp.period_end, os.type,
               CASE WHEN og.parent_id IS NOT NULL THEN 'ИПО' ELSE 'ОПО' END,
               CASE WHEN og.parent_id IS NOT NULL THEN 1 ELSE 2 END) cs
         LEFT JOIN dwh_bi.monitor_sor_soch_t2 ms
                   ON ms.school_id = cs.school_id AND
                      ms.group_id = cs.group_id AND
                      ms.subject_id = cs.subject_id AND
                      ms.period_type = cs.period_type AND
                      ms.period = cs.period

WHERE cs.subgroup IS NULL
;


-- нарушения четверть кол-во СОР/СОЧ
CREATE MATERIALIZED VIEW dwh_mid.monitor_problem_quarter_count AS
SELECT t2.t2_id                               AS t2_id,
       CASE WHEN t2.count_sor > 3 THEN 1 END  AS pqc_sor,
       CASE WHEN t2.count_soch > 1 THEN 1 END AS pqc_soch
FROM dwh_bi.monitor_sor_soch_t2 t2
WHERE t2.period_type = 'quarter'
;
-- нарушения полугодие кол-во СОР/СОЧ
CREATE MATERIALIZED VIEW dwh_mid.monitor_problem_halfyear_count AS
SELECT t2.t2_id                               AS t2_id,
       CASE WHEN t2.count_sor > 2 THEN 1 END  AS phc_sor,
       CASE WHEN t2.count_soch > 0 THEN 1 END AS phc_soch
FROM dwh_bi.monitor_sor_soch_t2 t2
WHERE t2.period_type = 'halfyear'
;
-- нарушение СОР и СОЧ в один день
CREATE MATERIALIZED VIEW dwh_mid.monitor_problem_sor_soch AS
SELECT t1.t1_id                                AS t1_id,
       tc1.t1_id                               AS tc1_id,
       CASE WHEN t1.date = tc1.date THEN 1 END AS p_sor_soch
FROM dwh_bi.monitor_sor_soch_t1 t1
         INNER JOIN dwh_bi.monitor_sor_soch_t1 tc1 ON tc1.school_id = t1.school_id AND
                                                      tc1.group_id = t1.group_id AND
                                                      tc1.subject_id = t1.subject_id AND
                                                      tc1.period = t1.period
WHERE t1.type = 'sor'
  AND tc1.type = 'soch'
;
-- нарушение более 3 СОЧ
CREATE MATERIALIZED VIEW dwh_mid.monitor_problem_count_soch AS
SELECT cs.t1_id                            AS t1_id,
       CASE WHEN cs.pc_soch > 3 THEN 1 END AS pc_soch
FROM (SELECT t1.t1_id AS t1_id,
             COUNT(*) AS pc_soch
      FROM dwh_bi.monitor_sor_soch_t1 t1
               INNER JOIN dwh_bi.monitor_sor_soch_t1 tc1 ON tc1.school_id = t1.school_id AND
                                                            tc1.group_id = t1.group_id AND
                                                            tc1.period = t1.period AND
                                                            tc1.date = t1.date
      WHERE t1.type = 'soch'
        AND tc1.type = 'soch'
      GROUP BY t1.t1_id) cs
;
-- нарушение СОЧ последний день
CREATE MATERIALIZED VIEW dwh_mid.monitor_problem_date_soch AS
SELECT t1.t1_id                                     AS t1_id,
       CASE WHEN t1.date = t1.period_end THEN 1 END AS pd_soch
FROM dwh_bi.monitor_sor_soch_t1 t1
WHERE t1.type = 'soch'
;


-- мониторинг СОР/СОЧ нарушения
-- CREATE MATERIALIZED VIEW dwh_bi.monitor_sor_soch_t3 AS
SELECT tt1.t1_id      AS t1_id,
       tt1.t2_id      AS t2_id,
       pqc.pqc_sor    AS pqc_sor,
       pqc.pqc_soch   AS pqc_soch,
       phc.phc_sor    AS phc_sor,
       phc.phc_soch   AS phc_soch,
       pss.p_sor_soch AS pd_sor_soch,
       pcs.pc_soch    AS pc_soch,
       pds.pd_soch    AS pd_soch
FROM dwh_bi.monitor_sor_soch_t1 tt1

    -- нарушения четверть кол-во СОР/СОЧ
         LEFT JOIN dwh_mid.monitor_problem_quarter_count pqc ON pqc.t2_id = tt1.t2_id
    -- нарушения полугодие кол-во СОР/СОЧ
         LEFT JOIN dwh_mid.monitor_problem_halfyear_count phc ON phc.t2_id = tt1.t2_id
    -- нарушение СОР и СОЧ в один день
         LEFT JOIN dwh_mid.monitor_problem_sor_soch pss ON pss.t1_id = tt1.t1_id OR
                                                           pss.tc1_id = tt1.t1_id
    -- нарушение более 3 СОЧ в день
         LEFT JOIN dwh_mid.monitor_problem_count_soch pcs ON pcs.t1_id = tt1.t1_id
    -- нарушение СОЧ последний день
         LEFT JOIN dwh_mid.monitor_problem_date_soch pds ON pds.t1_id = tt1.t1_id

GROUP BY tt1.t1_id, tt1.t2_id, pqc.pqc_sor, pqc.pqc_soch, phc.phc_sor, phc.phc_soch, pss.p_sor_soch, pcs.pc_soch, pds.pd_soch
;


CREATE INDEX monitor_sor_soch_t2_id_index ON dwh_bi.monitor_sor_soch_t2 (t2_id);
CREATE INDEX monitor_sor_soch_t2_cpst_index ON dwh_bi.monitor_sor_soch_t2 (school_id, group_id, period, program_id, subject_id);
CREATE INDEX monitor_sor_soch_t1_id_index ON dwh_bi.monitor_sor_soch_t1 (t1_id);
CREATE INDEX monitor_sor_soch_t1_t2_id_index ON dwh_bi.monitor_sor_soch_t1 (t2_id);
CREATE INDEX monitor_sor_soch_t1_cpst_index ON dwh_bi.monitor_sor_soch_t1 (school_id, group_id, period, program_id, subject_id, date);

CREATE INDEX monitor_problem_quarter_count_id_index ON dwh_mid.monitor_problem_quarter_count (t2_id);
CREATE INDEX monitor_problem_halfyear_count_id_index ON dwh_mid.monitor_problem_halfyear_count (t2_id);
CREATE INDEX monitor_problem_sor_soch_t1_id_index ON dwh_mid.monitor_problem_sor_soch (t1_id);
CREATE INDEX monitor_problem_sor_soch_tc1_id_index ON dwh_mid.monitor_problem_sor_soch (tc1_id);
CREATE INDEX monitor_problem_count_soch_id_index ON dwh_mid.monitor_problem_count_soch (t1_id);
CREATE INDEX monitor_problem_date_soch_id_index ON dwh_mid.monitor_problem_date_soch (t1_id);


REFRESH MATERIALIZED VIEW dwh_bi.monitor_sor_soch_t3; -- 4
REFRESH MATERIALIZED VIEW dwh_bi.monitor_sor_soch_t2; -- 1
REFRESH MATERIALIZED VIEW dwh_bi.monitor_sor_soch_t1; -- 2
REFRESH MATERIALIZED VIEW dwh_mid.monitor_problem_halfyear_count; -- 3
REFRESH MATERIALIZED VIEW dwh_mid.monitor_problem_quarter_count; -- 3
REFRESH MATERIALIZED VIEW dwh_mid.monitor_problem_sor_soch; -- 3
REFRESH MATERIALIZED VIEW dwh_mid.monitor_problem_count_soch; -- 3
REFRESH MATERIALIZED VIEW dwh_mid.monitor_problem_date_soch; -- 3
