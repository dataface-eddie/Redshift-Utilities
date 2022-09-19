/******************************************************
Purpose:  The purpose of this query is to create an easy means to compare a metric that is being developed or QA'ed in a test table
          against the same metric in a control table.
How:      This is designed for the user to write their own high level test query and pull a single result for a test metric into the 
          test CTE. Then the user can write a similar control metric in the control CTE. The CTE's are the only thing that should
          need any updating prior to using.
Useage:   The output should provide 3 fields and 4 records. The seq field can be ignored as it is just a sequencing mechanism. The
          first row will provide the test data sample. The second row is the control. The third is the difference between the two.
          The fourth row is the percent difference between the two. The last is shown as a percentage (out of 100). 
          
*******************************************************/
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
