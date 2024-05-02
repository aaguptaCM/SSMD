-- QA 1: Row Count
SELECT COUNT(*)
FROM "claims"."workspace"."ssmd_composite_score" AS A;

-- QA : Provider Count
SELECT COUNT(DISTINCT PROVIDERID)
FROM "claims"."workspace"."ssmd_composite_score" AS A;

-- Unique rollups
SELECT COUNT(DISTINCT rollup_category_key) 
FROM "claims"."workspace"."ssmd_composite_score";

-- Count of scores by level of care
SELECT
    level,
    SUM(scorecount) AS total_scores
FROM (
    SELECT level, COUNT(*) AS scorecount
    FROM "claims"."workspace"."ssmd_formula_finalscore" AS A
    WHERE Scoreflag = 'Score'
    GROUP BY level
) AS groups
GROUP BY level;

-- Get aggregate of pillar scores for each provider
-- SELECT
--     COUNT(DISTINCT providerid) AS providcount
-- FROM (
--     SELECT
--         providerid,
--         -- rollup_category_key,
--         -- COUNT(providerid)
--         AVG(aoc_score) AS avgaoc,
--         AVG(relex_score) AS avgrelex,
--         AVG(pe_score) AS avgpe,
--         AVG(cost_score) AS avgcost,
--         AVG(cqm_final) AS avgcqm
--     FROM (
--         SELECT
--             providerid,
--             rollup_category_key,
--             MAX(aoc_score) AS aoc_score,
--             MAX(relex_score) AS relex_score,
--             MAX(pe_score) AS pe_score,
--             MAX(cost_score) AS cost_score,
--             MAX(cqm_final) AS cqm_final
--         FROM "claims"."workspace"."ssmd_formula_finalscore" AS A
--         WHERE SCOREFLAG = 'Score' AND rollup_category_key <> 485
--         GROUP BY
--             providerid,
--             rollup_category_key
--         ) AS groups
--     -- GROUP BY providerid;
--     -- WHERE providerid = 66408
--     GROUP BY providerid
-- ) AS final_query
-- WHERE avgaoc != 0;

-- Get count of scores by score band
SELECT *
    -- COUNT(*)
FROM "claims"."workspace"."ssmd_composite_score"
WHERE compscore IS NOT NULL AND rollup_category = 'OFFICE VISIT'
-- WHERE compscore > 90
-- WHERE compscore > 80 AND compscore <= 90
-- WHERE compscore > 70 AND compscore <= 80
-- WHERE compscore > 60 AND compscore <= 70
-- WHERE compscore > 50 AND compscore <= 60 
-- WHERE compscore <= 50








-- Count of scores across payergroups
-- SELECT 
--     payergroupnumber,
--     COUNT(DISTINCT CONCAT(providerid, rollup_category_key)) AS provrollup_count
-- FROM "claims"."workspace"."ssmd_composite_score"
-- GROUP BY payergroupnumber;

-- Count of providers with missing pillar scores - aoc
-- SELECT 
--     COUNT(DISTINCT A.providerid)
--     -- A.providerid,
--     -- A.aoc_score
--     -- A.rollup_category_key,
--     -- A.payergroupnumber,
--     -- A.compscore
--     -- COUNT(DISTINCT providerid || rollup_category_key || payergroupnumber) as provrolluppayer_group
-- FROM "claims"."workspace"."ssmd_formula_finalscore" AS A
-- WHERE scoreflag = 'Score' AND aoc_score != 0;
-- -- GROUP BY providerid, aoc_score
-- -- AND aoc_score != 0

-- -- Count of providers with scores - relative exp
-- SELECT 
--     COUNT(DISTINCT providerid || rollup_category_key || rollup_category_key || payergroupnumber) as provrollup_group
-- FROM "claims"."workspace"."ssmd_formula_finalscore" AS A
-- WHERE scoreflag = 'Score' AND relex_score != 0;

-- -- Count of providers with scores - pe
-- SELECT
--     COUNT(DISTINCT A.providerid)
--     -- COUNT(DISTINCT providerid || rollup_category_key || rollup_category_key || payergroupnumber) as provrollup_group
-- FROM "claims"."workspace"."ssmd_formula_finalscore" AS A
-- WHERE scoreflag = 'Score' AND pe_score != 61 AND rollup_category != 'OFFICE VISIT';

-- -- Count of providers with scores - cost
-- SELECT 
--     COUNT(DISTINCT providerid || payergroupnumber) as provpayer_group
-- FROM "claims"."workspace"."ssmd_formula_finalscore" AS A
-- WHERE scoreflag = 'Score' AND cost_score != 0;