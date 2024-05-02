-- Create the roster table for the SSMD Pillar Calcs
CREATE TABLE "claims"."workspace"."ssmd_roster1" AS
SELECT
    A.providerid,
    A.ismddo,
    COALESCE(CAST(B.specialtyid AS VARCHAR), '') AS specialtyid,
    COALESCE(CAST(C.specialtycode AS VARCHAR), '') AS specialtycode,
    COALESCE(CAST(D.rollup_category AS VARCHAR), '') AS rollup_category,
    COALESCE(CAST(D.rollup_category_key AS VARCHAR), '') AS rollup_category_key,
    COALESCE(CAST(E.level AS VARCHAR), '') AS level
FROM "claims"."ingenix".physician AS A
    LEFT JOIN "claims"."ingenix".providerspecialty AS B
        ON A.providerid = B.providerid
    LEFT JOIN "claims"."ingenix".specialty AS C
        ON B.specialtyid = C.specialtyid
    LEFT JOIN "claims"."relative_exp".rollupspecialty_to_rollupcategory AS D
        ON B.specialtyid = D.specialtyid
    LEFT JOIN "claims"."relative_exp".ccsrollup_lvlcare AS E
        ON D.rollup_category_key = E.rollup_category_key;

-- Change the level of care for providers who are non-MD/DOs in the defined rollups
CREATE TABLE "claims"."workspace"."ssmd_roster2" AS
SELECT DISTINCT
    A.providerid,
    A.ismddo,
    A.specialtyid,
    A.specialtycode,
    A.rollup_category,
    A.rollup_category_key,
    CASE
        WHEN NOT A.ismddo AND rollup_category_key IN ('461', '485', '304', '266', '99', '102', '148', '166') THEN '4'
        ELSE level
    END AS level
FROM "claims"."workspace"."ssmd_roster1" AS A;
-- WHERE level = '461';

-- QA query
-- SELECT *
-- FROM "claims"."workspace"."ssmd_roster2" AS A
-- WHERE rollup_category_key = '461' AND ismddo = FALSE;

-- Filter the final roster table
CREATE TABLE "claims"."workspace"."ssmd_roster3" AS
SELECT DISTINCT *
FROM "claims"."workspace"."ssmd_roster2" AS A
WHERE specialtyid != '' AND rollup_category != '';

-- Read in the cost data for payergroups and join to the roster table
CREATE TABLE "claims"."workspace"."ssmd_roster_final" AS
SELECT DISTINCT
    A.*,
    B.payergroup AS payergroupnumber
FROM "claims"."workspace"."ssmd_roster3" AS A
    LEFT JOIN "claims"."psi"."PhysicianMCI_03132023" AS B
        ON A.providerid = B.providerid;

-- QA
SELECT COUNT(DISTINCT PROVIDERID)
FROM "claims"."workspace"."ssmd_roster3" AS A;
-- WHERE providerid = '414666' AND rollup_category_key = '163';

-- Add full form domain names to CQM data
-- CREATE TABLE "claims"."motive20230925"."MotiveQualityDetailByDomain_new" AS 
-- SELECT
--     A.npi,
--     A.providerid,
--     A.measures,
--     A.avgmeasurescore,
--     -- A.performancemeasurescore,
--     B.domain
-- FROM "claims"."aoc"."MotiveQualityDetailByDomain" AS A
--     LEFT JOIN "claims"."motive20230925"."DomainLookup" AS b
--         ON A.domain = B.shortformdomain;

-- Read in all the pillar data
CREATE TABLE "claims"."workspace"."ssmd_pillarcalcs" AS
SELECT DISTINCT
    A.providerid,
    A.ismddo,
    A.specialtyid,
    A.rollup_category_key,
    A.rollup_category,
    A.level,
    COALESCE(A.payergroupnumber, '') AS payergroupnumber,
    -- B.domain,
    COALESCE(ROUND(B.domainscore, 2), 0.0) AS aoc_score,
    COALESCE(CAST(C.rank_combined AS VARCHAR), '') AS relex_score,
    CASE
        WHEN CAST(D._c7 AS VARCHAR) = '50' THEN '61'
        ELSE COALESCE(CAST(D._c7 AS VARCHAR), '')
    END AS pe_score,
    COALESCE(CAST(E.bestcomparativentile AS VARCHAR), '') AS cost_score,
    COALESCE(CAST(F.avgmeasurescore AS VARCHAR), '') AS CQM_FINAL
FROM "claims"."workspace"."ssmd_roster_final" AS A
    LEFT JOIN "claims"."aoc"."Motive_AOC_Scores_10062023" AS B
        ON A.providerid = B.providerid 
        -- AND A.specialtyid = B.specialtyid
    LEFT JOIN "claims"."relative_exp"."finalrank" AS C
        ON A.providerid = C.providerid 
        AND A.rollup_category_key = C.rollup_category_key
        AND A.rollup_category = C.rollup_category
    LEFT JOIN "claims"."healthnav_compassphs_s3"."patient_experience_ssmd_parquet" AS D
        ON A.providerid = D._c0
    LEFT JOIN "claims"."psi"."PhysicianMCI_03132023" AS E
        ON A.providerid = E.providerid
        AND A.specialtyid = E.specialtyid
        AND A.payergroupnumber = E.payergroup
    LEFT JOIN "claims"."aoc"."MotiveAvgQualityScores" AS F
        ON A.providerid = F.providerid;
-- WHERE cqm_final != ''
    -- AND A.providerid = '369341'
    -- AND A.rollup_category_key = '352'

-- SELECT COUNT(DISTINCT providerid)
-- FROM "claims"."workspace"."ssmd_pillarcalcs"

-- Get only the necessary columns
CREATE TABLE "claims"."workspace"."ssmd_pillarcalcs2" AS
SELECT
    A.providerid,
    A.ismddo,
    A.rollup_category_key,
    A.rollup_category,
    A.level,
    A.payergroupnumber,
    MAX(A.aoc_score) AS aoc_score,
    MAX(A.relex_score) AS relex_score,
    MAX(A.pe_score) AS pe_score,
    MAX(A.cost_score) AS cost_score,
    MAX(A.CQM_FINAL) AS CQM_FINAL
FROM "claims"."workspace"."ssmd_pillarcalcs" AS A
-- WHERE A.providerid = '369341'
--     AND A.rollup_category_key = '352'
GROUP BY
    A.providerid,
    A.ismddo,
    A.rollup_category_key,
    A.rollup_category,
    A.level,
    A.payergroupnumber;

-- SELECT COUNT(DISTINCT PROVIDERID)
-- FROM "claims"."workspace"."ssmd_pillarcalcs2"

--  QA: Get counts of payergroups by providerid/ccs rollup combination
-- CREATE TABLE "claims"."workspace"."ssmd_test_rowcount" AS 
-- SELECT DISTINCT
--     A.providerid,
--     A.rollup_category_key,
--     COUNT(*) AS row_count    
-- FROM "claims"."workspace"."ssmd_pillarcalcs2" AS A
-- GROUP BY 
--     A.providerid,
--     A.rollup_category_key
-- HAVING COUNT(*) < 6
-- ;

-- Filter table to missing payergroups
CREATE TABLE "claims"."workspace"."ssmd_pillarcalcs2_missing" AS
SELECT
    A.*
FROM "claims"."workspace"."ssmd_pillarcalcs2" AS A
WHERE payergroupnumber = '';

-- Filter table to not missing payergroups
CREATE TABLE "claims"."workspace"."ssmd_pillarcalcs2_notmissing" AS
SELECT
    A.*
FROM "claims"."workspace"."ssmd_pillarcalcs2" AS A
WHERE payergroupnumber != '';

-- CTEs for the payergroup cross join
-- all the payer group numbers to be mapped
CREATE TABLE "claims"."workspace"."ssmd_pillarcalcs_pygfilled" AS
WITH payer_groups AS (
    SELECT '001' AS payergroupnumber UNION ALL 
    SELECT '003' AS payergroupnumber UNION ALL
    SELECT '005' AS payergroupnumber UNION ALL
    SELECT '524' AS payergroupnumber UNION ALL
    SELECT 'MLP' AS payergroupnumber UNION ALL
    SELECT 'UPG' AS payergroupnumber
)
SELECT 
    A.providerid,
    A.ismddo,
    A.rollup_category_key,
    A.rollup_category,
    A.level,
    B.payergroupnumber,
    A.aoc_score,
    A.relex_score,
    A.pe_score,
    A.cost_score,
    A.CQM_FINAL
FROM "claims"."workspace"."ssmd_pillarcalcs2_missing" AS A
    CROSS JOIN payer_groups AS B;


-- Combine the missing payergroups and the filled payergroup tables
CREATE TABLE "claims"."workspace"."ssmd_pillarcalcs_complete" AS
SELECT DISTINCT *
FROM "claims"."workspace"."ssmd_pillarcalcs_pygfilled"
UNION ALL
SELECT DISTINCT *
FROM "claims"."workspace"."ssmd_pillarcalcs2_notmissing";

--  QA: Get counts of payergroups by providerid/ccs rollup combination
-- CREATE TABLE "claims"."workspace"."ssmd_test_rowcount" AS 
-- SELECT DISTINCT
--     A.providerid,
--     A.rollup_category_key,
--     COUNT(*) AS row_count    
-- FROM "claims"."workspace"."ssmd_pillarcalcs_pygfilled" AS A
-- GROUP BY 
--     A.providerid,
--     A.rollup_category_key
-- HAVING COUNT(*) < 6
-- ;

-- QA
-- Row count: 64859889, 
-- providerid, rollup, payergroup combination count: 64859889
-- SELECT 
--    COUNT(*) AS number_of_groups 
-- FROM (
--     SELECT A.providerid, A.rollup_category_key, A.payergroupnumber
--     FROM "claims"."workspace"."ssmd_pillarcalcs2" AS A
--     GROUP BY A.providerid, A.rollup_category_key, A.payergroupnumber
-- ) AS grouped_data;

-- QA
-- row count: 65687514
-- providerid, rollup, payergroup combination count: 65687514
-- SELECT COUNT(*)
-- FROM "claims"."workspace"."ssmd_pillarcalcs_complete" AS A;
-- SELECT 
--    COUNT(*) AS number_of_groups 
-- FROM (
--     SELECT A.providerid, A.rollup_category_key, A.payergroupnumber
--     FROM "claims"."workspace"."ssmd_pillarcalcs_complete" AS A
--     GROUP BY A.providerid, A.rollup_category_key, A.payergroupnumber
-- ) AS grouped_data;

-- Fill in the pillars with missing values
CREATE TABLE "claims"."workspace"."ssmd_pillarcalcs_complete2" AS
SELECT
    A.providerid,
    A.ismddo,
    A.rollup_category_key,
    A.rollup_category,
    A.level,
    A.payergroupnumber,
    A.aoc_score,
    CASE
        WHEN A.relex_score = '' THEN '0'
        ELSE A.relex_score
    END AS relex_score,
    CASE
        WHEN A.pe_score = '' THEN '61'
        ELSE A.pe_score
    END AS pe_score,
    CASE
        WHEN A.cost_score = '' THEN '0'
        ELSE A.cost_score
    END AS cost_score,
    CASE
        WHEN A.CQM_FINAL = '' THEN '0.61'
        ELSE A.CQM_FINAL
    END AS CQM_FINAL
FROM "claims"."workspace"."ssmd_pillarcalcs_complete" AS A;

-- Create the logic for the ScoreFlagFinal
CREATE TABLE "claims"."workspace"."ssmd_pillarcalcs_scoreflag" AS
SELECT 
    A.*,
    CASE
        WHEN A.ismddo = True AND A.rollup_category = 'OFFICE VISIT' THEN 'Score'
        WHEN A.ismddo = True AND A.level = '1' AND A.Relex_Score > '0' AND A.CQM_FINAL > '0' AND A.pe_score > '0' AND A.Cost_score > '0' AND A.rollup_category != 'OFFICE VISIT' THEN 'Score'
        WHEN A.ismddo = True AND A.level = '2' AND A.Relex_Score > '0' AND A.CQM_FINAL > '0' AND A.Cost_score > '0' AND A.rollup_category != 'OFFICE VISIT' THEN 'Score'
        WHEN A.ismddo = True AND A.level = '3' AND A.Relex_Score > '0' AND A.CQM_FINAL > '0' AND A.rollup_category != 'OFFICE VISIT' THEN 'Score'
        WHEN A.level = '4' AND A.Relex_Score > '0' AND A.pe_score > '0' AND A.Cost_score > '0' AND A.rollup_category != 'OFFICE VISIT' THEN 'Score'
        ELSE 'No Score'
    END AS ScoreFlag
FROM "claims"."workspace"."ssmd_pillarcalcs_complete2" AS A;
-- WHERE ScoreFlag = 'Score' AND A.level = '4';

-- Logic to calculate the denominator based on the missing values for pillars
CREATE TABLE  "claims"."workspace"."ssmd_pillarcalcs_denom" AS
SELECT DISTINCT
    A.*,
    CASE
        WHEN A.ScoreFlag = 'Score' AND A.level = '1' AND A.relex_score = '0' THEN 20
        WHEN A.ScoreFlag = 'Score' AND A.level = '2' AND A.relex_score = '0' THEN 35
        WHEN A.ScoreFlag = 'Score' AND A.level = '3' AND A.relex_score = '0' THEN 45
        WHEN A.ScoreFlag = 'Score' AND A.level = '4' AND A.relex_score = '0' THEN 60 
        ELSE 0
    END AS relex_score_missing,
    CASE
        WHEN A.ScoreFlag = 'Score' AND A.level = '1' AND A.CQM_FINAL = '0' THEN 35
        WHEN A.ScoreFlag = 'Score' AND A.level = '2' AND A.CQM_FINAL = '0' THEN 30
        WHEN A.ScoreFlag = 'Score' AND A.level = '3' AND A.CQM_FINAL = '0' THEN 45
        WHEN A.ScoreFlag = 'Score' AND A.level = '4' AND A.CQM_FINAL = '0' THEN 0 
        ELSE 0
    END AS CQM_Score_missing,
    CASE
        WHEN A.ScoreFlag = 'Score' AND A.level = '1' AND A.pe_score = '0' THEN 15
        WHEN A.ScoreFlag = 'Score' AND A.level = '2' AND A.pe_score = '0' THEN 5
        WHEN A.ScoreFlag = 'Score' AND A.level = '3' AND A.pe_score = '0' THEN 0
        WHEN A.ScoreFlag = 'Score' AND A.level = '4' AND A.pe_score = '0' THEN 25 
        ELSE 0
    END AS PE_Score_missing,
    CASE
        WHEN A.ScoreFlag = 'Score' AND A.level = '1' AND A.aoc_score = '0' THEN 10
        WHEN A.ScoreFlag = 'Score' AND A.level = '2' AND A.aoc_score = '0' THEN 10
        WHEN A.ScoreFlag = 'Score' AND A.level = '3' AND A.aoc_score = '0' THEN 10
        WHEN A.ScoreFlag = 'Score' AND A.level = '4' AND A.aoc_score = '0' THEN 0
        ELSE 0
    END AS AOC_Score_missing,
    CASE
        WHEN A.ScoreFlag = 'Score' AND A.level = '1' AND A.cost_score = '0' THEN 20
        WHEN A.ScoreFlag = 'Score' AND A.level = '2' AND A.cost_score = '0' THEN 20
        WHEN A.ScoreFlag = 'Score' AND A.level = '3' AND A.cost_score = '0' THEN 0
        WHEN A.ScoreFlag = 'Score' AND A.level = '4' AND A.cost_score = '0' THEN 15 
        ELSE 0
    END AS Cost_Score_missing,
    relex_score_missing + CQM_Score_missing + PE_Score_missing + AOC_Score_missing + Cost_Score_missing AS MissingScores,
    (100 - MissingScores) AS Den
FROM "claims"."workspace"."ssmd_pillarcalcs_scoreflag" AS A;
-- WHERE A.ScoreFlag = 'Score' AND A.level != '' AND A.level != '3' AND A.cost_score = '0';
-- WHERE Den < 90;

-- Create a table with weights for the scoring - does not need to be run after first run unless there are updates to the weights
CREATE TABLE "claims"."workspace"."ssmd_levelofcare_weights" (
    Level INT,
    RelexWeight DECIMAL(3,2),
    QMWeight DECIMAL(3,2),
    PEWeight DECIMAL(3,2),
    AOCWeight DECIMAL(3,2),
    CostWeight DECIMAL(3,2),
    ActiveFlag BOOLEAN,
    Date DATE
);

INSERT INTO "claims"."workspace"."ssmd_levelofcare_weights" (Level, RelexWeight, QMWeight, PEWeight, AOCWeight, CostWeight, ActiveFlag, Date)
VALUES
    (1, 0.20, 0.35, 0.15, 0.10, 0.20, 1, CURRENT_DATE),
    (2, 0.35, 0.30, 0.05, 0.10, 0.20, 1, CURRENT_DATE),
    (3, 0.45, 0.45, 0.00, 0.10, 0.00, 1, CURRENT_DATE),
    (4, 0.60, 0.00, 0.25, 0.00, 0.15, 1, CURRENT_DATE);

SELECT * FROM "claims"."workspace"."ssmd_levelofcare_weights";



-- Calculate the composite scores
CREATE TABLE "claims"."workspace"."ssmd_pillarcalcs_compscore" AS
SELECT 
    A.*,
    CASE 
        WHEN scoreflag = 'Score' THEN 
            (A.relex_score * B.RelexWeight) + 
            (A.cqm_final * B.QMWeight) + 
            (A.pe_score * B.PEWeight) + 
            (A.aoc_score * B.AOCWeight) + 
            (A.cost_score * B.CostWeight)
    END AS Num,
    -- CASE
    --     WHEN scoreflag = 'Score' THEN
    --         ((A.relex_score * B.RelexWeight) + 
    --         (A.cqm_final * B.QMWeight) + 
    --         (A.pe_score * B.PEWeight) + 
    --         (A.aoc_score * B.AOCWeight) + 
    --         (A.cost_score * B.CostWeight)) / A.den
    -- END AS compscore
    -- CASE
    --     WHEN A.level = '1' AND A.ScoreFlag = 'Score' THEN ((0.2*A.relex_score) + (0.35*A.CQM_FINAL) + (0.15*pe_score) + (0.1*A.aoc_score) + (0.2*A.cost_score))
    --     WHEN A.level = '2' AND A.ScoreFlag = 'Score' THEN ((0.35*A.relex_score) + (0.30*A.CQM_FINAL) + (0.05*pe_score) + (0.1*A.aoc_score) + (0.2*A.cost_score))
    --     WHEN A.level = '3' AND A.ScoreFlag = 'Score' THEN ((0.45*A.relex_score) + (0.45*A.CQM_FINAL) + (0*pe_score) + (0.1*A.aoc_score) + (0*A.cost_score))
    --     WHEN A.level = '4' AND A.ScoreFlag = 'Score' THEN ((0.6*A.relex_score) + (0*A.CQM_FINAL) + (0.25*pe_score) + (0*A.aoc_score) + (0.15*A.cost_score))
    -- END AS Num,
    Num/A.Den AS CompScore
FROM "claims"."workspace"."ssmd_pillarcalcs_denom" AS A
    LEFT JOIN "claims"."workspace"."ssmd_levelofcare_weights" AS B
        ON A.level = B.level;

-- SELECT *
-- FROM "claims"."workspace"."ssmd_pillarcalcs_compscore" AS A
-- WHERE compscore > 0.90;

-- QA
-- SELECT COUNT(DISTINCT providerid)
-- FROM "claims"."workspace"."ssmd_pillarcalcs_compscore";
-- -- WHERE scoreflag = 'Score'
-- WHERE providerid = 1659797
-- AND scoreflag = 'Score';

-- Create a new table for Office Visits with the necessary flags
CREATE TABLE "claims"."workspace"."finalofficevisit_flags" AS
SELECT DISTINCT
    A.*,
    B.specialtyid,
    C.rollup_category_key AS other_rollups,
    CASE
        WHEN other_rollups = 515 THEN 1 ELSE 0
    END AS BH_FLAG,
    CASE
        WHEN other_rollups IN (486, 487, 488, 489, 490, 491, 492, 493, 494, 495) THEN 1 ELSE 0
    END AS FM_FLAG,
    CASE
        WHEN other_rollups IN (496, 497, 498, 499, 500, 501, 502, 503, 504) THEN 1 ELSE 0
    END AS PEDS_FLAG,
    CASE
        WHEN BH_FLAG = 0 AND FM_FLAG = 0 AND PEDS_FLAG = 0 THEN 1 ELSE 0
    END AS UNCAT_FLAG
FROM
    "claims"."relative_exp"."finalofficevisit" AS A
    LEFT JOIN 
        "claims"."ingenix".providerspecialty AS B
            ON A.providerid = B.providerid
        LEFT JOIN
            "claims"."relative_exp"."rollupspecialty_to_rollupcategory" AS C
                ON B.specialtyid = C.specialtyid;

-- Consolidate the table to get flags by providerid
CREATE TABLE "claims"."workspace"."finalofficevisit_flags2" AS
SELECT
    providerid,
    MAX(bh_flag) AS bh_flag,
    MAX(FM_FLAG) AS fm_flag,
    MAX(peds_flag) AS peds_flag,
    MAX(uncat_flag) AS uncat_flag
FROM
    "claims"."relative_exp"."finalofficevisit_flags"
GROUP BY providerid;

-- Filter table to calculate the Office Visit scores
CREATE TABLE "claims"."workspace"."ssmd_offVisits" AS
SELECT
    A.*,
    B.bh_flag,
    B.fm_flag, 
    B.peds_flag,
    B.uncat_flag
FROM "claims"."workspace"."ssmd_pillarcalcs_compscore" AS A
    LEFT JOIN "claims"."relative_exp"."finalofficevisit_flags2" AS B
        ON A.providerid = B.providerid
WHERE A.ScoreFlag = 'Score';

-- Calculate the FM office visit scores - avg top 4 scores
CREATE TABLE "claims"."workspace"."ssmd_offVisits_fmOffVisits" AS
SELECT
    A.providerid,
    B.AvgTop4FM
FROM "claims"."workspace"."ssmd_offVisits" AS A
    LEFT JOIN (
        SELECT
            providerid,
            fm_flag,
            AVG(CompScore) AS AvgTop4FM
        FROM (
            SELECT providerid, compscore, fm_flag,
                ROW_NUMBER() OVER (PARTITION BY providerid ORDER BY compscore DESC) AS rn
            FROM "claims"."workspace"."ssmd_offVisits"
            WHERE fm_flag = 1 AND compscore IS NOT NULL
        ) AS subquery
        WHERE rn <= 4
        GROUP BY 
            providerid, fm_flag
    ) AS B ON A.providerid = B.providerid
WHERE A.fm_flag = 1
GROUP BY 
    A.providerid,
    B.AvgTop4FM;

-- Calculate the BH office visit scores - avg all scores
CREATE TABLE "claims"."workspace"."ssmd_offVisits_bhOffVisits" AS
SELECT
    A.providerid,
    AVG(A.CompScore) AS AvgBHCompScore
FROM "claims"."workspace"."ssmd_offVisits" AS A
WHERE A.bh_flag = 1
GROUP BY 
    A.providerid;

-- Calculate the Peds office visit scores - avg top 4 scores
CREATE TABLE "claims"."workspace"."ssmd_offVisits_pedsOffVisits" AS
SELECT
    A.providerid,
    B.AvgTop4Ped
FROM "claims"."workspace"."ssmd_offVisits" AS A
    LEFT JOIN (
        SELECT
            providerid,
            peds_flag,
            AVG(CompScore) AS AvgTop4Ped
        FROM (
            SELECT providerid, compscore, peds_flag,
                ROW_NUMBER() OVER (PARTITION BY providerid ORDER BY compscore DESC) AS rn
            FROM "claims"."workspace"."ssmd_offVisits"
            WHERE peds_flag = 1 AND compscore IS NOT NULL
        ) AS subquery
        WHERE rn <= 4
        GROUP BY 
            providerid, peds_flag
    ) AS B ON A.providerid = B.providerid
WHERE A.peds_flag = 1
GROUP BY 
    A.providerid,
    B.AvgTop4Ped;

-- Calculate the uncat office visit scores - avg top 5 scores
CREATE TABLE "claims"."workspace"."ssmd_offVisits_uncatOffVisits" AS
SELECT
    A.providerid,
    B.AvgTop5Uncat
FROM "claims"."workspace"."ssmd_offVisits" AS A
    LEFT JOIN (
        SELECT
            providerid,
            uncat_flag,
            AVG(CompScore) AS AvgTop5Uncat
        FROM (
            SELECT providerid, compscore, uncat_flag,
                ROW_NUMBER() OVER (PARTITION BY providerid ORDER BY compscore DESC) AS rn
            FROM "claims"."workspace"."ssmd_offVisits"
            WHERE uncat_flag = 1 AND compscore IS NOT NULL
        ) AS subquery
        WHERE rn <= 5
        GROUP BY 
            providerid, uncat_flag
    ) AS B ON A.providerid = B.providerid
WHERE A.uncat_flag = 1
GROUP BY 
    A.providerid,
    B.AvgTop5Uncat;

-- Join all of the office visit score tables
CREATE TABLE "claims"."workspace"."ssmd_offVisits_finalScore" AS
SELECT
    A.providerid,
    B.AvgTop4FM,
    C.AvgBHCompScore,
    D.AvgTop4Ped,
    E.AvgTop5Uncat,
    COALESCE(B.AvgTop4FM, C.AvgBHCompScore, D.AvgTop4Ped, E.AvgTop5Uncat) AS OfficeVisitScore,
    '' AS payergroupnumber
FROM "claims"."workspace"."ssmd_offVisits" AS A
    LEFT JOIN "claims"."workspace"."ssmd_offVisits_fmOffVisits" AS B
        ON A.providerid = B.providerid
    LEFT JOIN "claims"."workspace"."ssmd_offVisits_bhOffVisits" AS C
        ON A.providerid = C.providerid
    LEFT JOIN "claims"."workspace"."ssmd_offVisits_pedsOffVisits" AS D
        ON A.providerid = D.providerid
    LEFT JOIN "claims"."workspace"."ssmd_offVisits_uncatOffVisits" AS E
        ON A.providerid = E.providerid;

-- Join in the ismddo flags to the table
CREATE TABLE "claims"."workspace"."ssmd_offVisits_finalScore2" AS
SELECT DISTINCT
    A.*,
    B.ismddo
FROM "claims"."workspace"."ssmd_offVisits_finalScore" AS A
    LEFT JOIN "claims"."workspace"."ssmd_offVisits" AS B
        ON A.providerid = B.providerid;

-- QA
-- SELECT *
-- FROM "claims"."workspace"."ssmd_offVisits_finalScore2"
-- -- WHERE scoreflag = 'Score'
-- WHERE providerid = 2628533;

-- Fill in the payergroups to the office visit TABLE
CREATE TABLE "claims"."workspace"."ssmd_offVisits_finalScorepyg" AS
WITH payer_groups AS (
    SELECT '001' AS payergroupnumber UNION ALL 
    SELECT '003' AS payergroupnumber UNION ALL
    SELECT '005' AS payergroupnumber UNION ALL
    SELECT '524' AS payergroupnumber UNION ALL
    SELECT 'MLP' AS payergroupnumber UNION ALL
    SELECT 'UPG' AS payergroupnumber
)
SELECT 
    A.providerid,
    A.ismddo,
    -- '' AS specialtyid,
    '485' AS rollup_category_key,
    'OFFICE VISIT' as rollup_category,
    '' AS level,
    B.payergroupnumber,
    -- '' AS domain,
    0.0 AS aoc_score,
    '0.0' AS relex_score,
    '0.0' AS pe_score,
    '0.0' AS cost_score,
    '0.0' AS cqm_final,
    'Score' AS scoreflag,
    0 AS relex_score_missing,
    0 AS cqm_score_missing,
    0 AS pe_score_missing,
    0 AS aoc_score_missing,
    0 AS cost_score_missing,
    0 AS missingscores,
    0 AS den,
    0 AS num,
    -- A.AvgTop4FM,
    -- A.AvgBHCompScore,
    -- A.AvgTop4Ped,
    -- A.AvgTop5Uncat,
    A.OfficeVisitScore AS compscore
FROM "claims"."workspace"."ssmd_offVisits_finalScore2" AS A
    CROSS JOIN payer_groups AS B;

-- QA
-- SELECT *
-- FROM "claims"."workspace"."ssmd_offVisits_finalScorepyg"
-- WHERE providerid = 2628533;

-- Join the office visit table to the overall scores table - RAW TABLE
CREATE TABLE "claims"."workspace"."ssmd_formula_finalscore" AS
SELECT *
FROM "claims"."workspace"."ssmd_pillarcalcs_compscore"
WHERE rollup_category != 'OFFICE VISIT'
UNION
SELECT *
FROM "claims"."workspace"."ssmd_offVisits_finalScorepyg";

-- QA
-- SELECT *
-- FROM "claims"."workspace"."ssmd_formula_finalscore"
-- WHERE providerid = 2628533;

-- Create the CompositeScore Table
CREATE TABLE "claims"."workspace"."ssmd_composite_score" AS
SELECT
    A.providerid,
    -- A.ismddo,
    -- A.scoreflag,
    A.rollup_category_key,
    A.rollup_category,
    A.payergroupnumber,
    A.compscore * 100 AS compscore
FROM "claims"."workspace"."ssmd_formula_finalscore" AS A
-- WHERE providerid = 2628533
-- AND rollup_category = 'OFFICE VISIT';
WHERE Scoreflag = 'Score'
AND compscore IS NOT NULL;