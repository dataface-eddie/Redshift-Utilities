WITH test AS(
    SELECT SUM(COALESCE(<DATA>, 0)) AS data
        , 'TEST - <metric_name>' AS metric
    FROM <test.table>
    GROUP BY 2
), 
CONTROL AS (
        SELECT SUM(COALESCE(<DATA>, 0)) AS data
        , 'CONTROL - <metric_name>' AS metric
    FROM <control.table>
    GROUP BY 2
),
base AS (
    SELECT *
        , 1 AS seq
    FROM test
    UNION ALL
    SELECT *
        , 2 AS seq
    FROM CONTROL
    )
SELECT *
FROM base
UNION ALL
SELECT DISTINCT FIRST_VALUE(DATA)OVER(ORDER BY seq rows between unbounded preceding and unbounded following) 
        - LAST_VALUE(DATA)OVER(ORDER BY seq rows between unbounded preceding and unbounded following)
    , 'Difference' AS metric
    , 3 AS seq
FROM base
UNION ALL
SELECT DISTINCT 
        (FIRST_VALUE(DATA)OVER(ORDER BY seq rows between unbounded preceding and unbounded following) 
            - LAST_VALUE(DATA)OVER(ORDER BY seq rows between unbounded preceding and unbounded following)) 
        / FIRST_VALUE(DATA)OVER(ORDER BY seq rows between unbounded preceding and unbounded following) *100
    , 'Percent Difference' AS metric
    , 4 AS seq
FROM base
ORDER BY seq
; 
