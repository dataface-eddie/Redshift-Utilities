/******************************************************
Purpose:  The purpose of this query is to easily generate all of the ddl required to make updates to a base table
          including any dependent view logic. 
          
How:      Using a CTE as a phony declared variable, the user inputs a table that they need to update the DDL on. The script
          then uses the pg catalog to script a query to ALTER TABLE on the existing table, renaming it with the 'dep'(deprecation)
          and date tag. Then, it generates the formatted table DDL, which the user can update as needed. The script then generates 
          UNLOAD and COPY statements to transport the data from the old table into the new one. (This step requires S3 access and 
          keys)Following that, the script generates any view DDL of dependent views on your table, as these will need to be
          repointed to your new table. Doing things in this order minimizes downtime of your views accessibility. Finally,
          the dep table is dropped.

Usage:   This is handy for increasing numeric/character limits on fields or adding or removing fields as well, especially when you
          have view dependencies in play. The user just needs to enter the schema.table of the table they want to update, then enter
          S3 specifications in the UNLOAD and COPY statements. Run the script. The output will be a series of queries that alter the
          table name, create the new table (still needs user updates made), move the data to the new table, repoint dependent views,
          and drop the dep table. Make any updates to your Table DDL that you need. If you add or remove any fields, be sure to update 
          the COPY options to match. When loading new fields, since they won't have values from the old table, you will need to 
          enumerate your own value (or set to NULL) within the COPY options. 
          
*******************************************************/
--DROP VIEW IF EXISTS admin.v_alter_table_script;
--CREATE OR REPLACE VIEW admin.v_alter_table_script
--AS
WITH user_defined_values
AS (
SELECT 
--FILL OUT DATA-SET PARAMETER BELOW WITH BASE TABLE THAT NEEDS TO BE CHANGE AND THEN EXECUTE ENTIRE QUERY
'<schema.tablename>'   --FORMAT ~ schema.tablename 
---------------------------
    ::TEXT AS target_data_set 
)
    
SELECT ddl::VARCHAR(50000)
FROM (
    SELECT schemaname
        ,tablename
        ,seq
        ,ddl
    FROM (

--ALTER TABLE
        SELECT n.nspname AS schemaname
            ,c.relname AS tablename
            ,0 AS seq
            ,'ALTER TABLE ' || QUOTE_IDENT(n.nspname) || '.' || QUOTE_IDENT(c.relname) || '\n' || 
            'RENAME TO ' || c.relname || '_dep_' || REPLACE(GETDATE()::DATE, '-', '_') || ';' AS ddl
        FROM pg_namespace AS n
        JOIN pg_class AS c 
            ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'

--CREATE TABLE
        UNION 
        SELECT n.nspname AS schemaname
            ,c.relname AS tablename
            ,2 AS seq
            ,'CREATE TABLE IF NOT EXISTS ' || QUOTE_IDENT(n.nspname) || '.' || QUOTE_IDENT(c.relname) || '' AS ddl
        FROM pg_namespace AS n
        JOIN pg_class AS c 
            ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'

--OPEN PAREN COLUMN LIST
        UNION 
        SELECT n.nspname AS schemaname
            , c.relname AS tablename
            , 5 AS seq
            , '(' AS ddl
        FROM pg_namespace AS n
        JOIN pg_class AS c 
            ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'

--COLUMN LIST
        UNION 
        SELECT schemaname
            ,tablename
            ,seq
            ,'\t' || col_delim || col_name || ' ' || col_datatype AS ddl
        FROM (
            SELECT n.nspname AS schemaname
                ,c.relname AS tablename
                ,1000 + a.attnum AS seq
                ,CASE 
                    WHEN a.attnum > 1 
                        THEN ',' 
                    ELSE '' 
                END AS col_delim
                ,QUOTE_IDENT(a.attname) AS col_name
                ,CASE 
                    WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING') > 0
                        THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING', 'VARCHAR')
                    WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER') > 0
                        THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER', 'CHAR')
                    ELSE UPPER(format_type(a.atttypid, a.atttypmod))
                END AS col_datatype
            FROM pg_namespace AS n
            JOIN pg_class AS c 
                ON n.oid = c.relnamespace
            JOIN pg_attribute AS a 
                ON c.oid = a.attrelid
            LEFT JOIN  pg_attrdef AS adef 
                ON a.attrelid = adef.adrelid 
                    AND a.attnum = adef.adnum
            WHERE c.relkind = 'r'
                AND a.attnum > 0
            ORDER BY a.attnum
            )

--CLOSE PAREN COLUMN LIST
        UNION 
        SELECT n.nspname AS schemaname
            , c.relname AS tablename
            , 20000 AS seq, ')' AS ddl
        FROM pg_namespace AS n
        JOIN pg_class AS c 
            ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'

--DISTSTYLE
        UNION 
        SELECT n.nspname AS schemaname
            ,c.relname AS tablename
            ,30000 AS seq
            ,CASE 
                WHEN c.reldiststyle = 0 
                    THEN 'DISTSTYLE EVEN'
                WHEN c.reldiststyle = 1 
                    THEN 'DISTSTYLE KEY'
                WHEN c.reldiststyle = 8 
                    THEN 'DISTSTYLE ALL'
                ELSE 'DISTSTYLE EVEN'
            END AS ddl
        FROM pg_namespace AS n
        JOIN pg_class AS c 
            ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'

--DISTKEY COLUMNS
        UNION 
        SELECT n.nspname AS schemaname
           ,c.relname AS tablename
           ,400000 + a.attnum AS seq
           ,'DISTKEY (' || QUOTE_IDENT(a.attname) || ')' AS ddl
        FROM pg_namespace AS n
        JOIN pg_class AS c 
            ON n.oid = c.relnamespace
        JOIN pg_attribute AS a 
            ON c.oid = a.attrelid
        WHERE c.relkind = 'r'
            AND a.attisdistkey IS TRUE
            AND a.attnum > 0

--SORTKEY COLUMNS
        UNION 
        SELECT schemaname
            ,tablename
            ,seq
            ,CASE 
                WHEN min_sort <0 
                    THEN 'INTERLEAVED SORTKEY (' 
                ELSE 'SORTKEY (' 
            END AS ddl
        FROM (
            SELECT n.nspname AS schemaname
                ,c.relname AS tablename
                ,500000 AS seq
                ,MIN(attsortkeyord) AS min_sort 
            FROM pg_namespace AS n
            JOIN  pg_class AS c 
                ON n.oid = c.relnamespace
            JOIN pg_attribute AS a 
                ON c.oid = a.attrelid
            WHERE c.relkind = 'r'
                AND abs(a.attsortkeyord) > 0
                AND a.attnum > 0
            GROUP BY n.nspname
                , c.relname)
        UNION (
        SELECT n.nspname AS schemaname
            ,c.relname AS tablename
            ,600000 + ABS(a.attsortkeyord) AS seq
            ,CASE 
                WHEN abs(a.attsortkeyord) = 1
                    THEN '\t' || QUOTE_IDENT(a.attname)
                ELSE '\t, ' || QUOTE_IDENT(a.attname)
            END AS ddl
        FROM  pg_namespace AS n
        JOIN pg_class AS c 
            ON n.oid = c.relnamespace
        JOIN pg_attribute AS a    
            ON c.oid = a.attrelid
        WHERE c.relkind = 'r'
            AND abs(a.attsortkeyord) > 0
            AND a.attnum > 0
        ORDER BY abs(a.attsortkeyord))
        UNION 
        SELECT n.nspname AS schemaname
            ,c.relname AS tablename
            ,700000 AS seq
            ,'\t)' AS ddl
        FROM pg_namespace AS n
        JOIN  pg_class AS c 
            ON n.oid = c.relnamespace
        JOIN  pg_attribute AS a 
            ON c.oid = a.attrelid
        WHERE c.relkind = 'r'
            AND ABS(a.attsortkeyord) > 0
            AND a.attnum > 0

--END SEMICOLON
        UNION 
        SELECT n.nspname AS schemaname
            , c.relname AS tablename
            , 800000 AS seq
            , ';' AS ddl
        FROM  pg_namespace AS n
        JOIN pg_class AS c 
            ON n.oid = c.relnamespace
        WHERE c.relkind = 'r' 

--COMPLETE UNLOAD STATEMENT
        UNION
        SELECT schemaname
            , tablename
            , seq
            , ddl
        FROM (
--BEGIN UNLOAD STATEMENT

            SELECT n.nspname AS schemaname
                    , c.relname AS tablename
                    , 900000 AS seq
                    , 'UNLOAD (' ||'''' || 'SELECT ' AS ddl
            FROM  pg_namespace AS n
            JOIN pg_class AS c 
                ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'

--UNLOAD CCOLUMN LIST      
            UNION    
            SELECT schemaname
                ,tablename
                ,seq
                ,'\t' || col_delim || col_name AS ddl
            FROM (
                SELECT n.nspname AS schemaname
                    ,c.relname AS tablename
                    ,1000000 + a.attnum AS seq
                    ,CASE 
                        WHEN a.attnum > 1 
                            THEN ',' 
                        ELSE '' 
                    END AS col_delim
                    ,QUOTE_IDENT(a.attname) AS col_name
                FROM pg_namespace AS n
                JOIN pg_class AS c 
                    ON n.oid = c.relnamespace
                JOIN pg_attribute AS a 
                    ON c.oid = a.attrelid
                LEFT JOIN  pg_attrdef AS adef 
                    ON a.attrelid = adef.adrelid 
                        AND a.attnum = adef.adnum
                WHERE c.relkind = 'r'
                    AND a.attnum > 0
                ORDER BY a.attnum
                )
            
--UNLOAD FROM STATEMENT
            UNION 
            SELECT n.nspname AS schemaname
                ,c.relname AS tablename
                ,1100000 AS seq
                ,'FROM ' || QUOTE_IDENT(n.nspname) || '.' || QUOTE_IDENT(c.relname) || '_dep_' || REPLACE(GETDATE()::DATE, '-', '_') || ''''  AS ddl
            FROM pg_namespace AS n
            JOIN pg_class AS c 
                ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
            )
--END UNLOAD STATEMENT
        UNION
        SELECT n.nspname AS schemaname
            , c.relname AS tablename
            , 1200000 AS seq
            , ')' || '\n' ||
            'TO ' || QUOTE_LITERAL('s3://<your_bucket/your_path>' || n.nspname || '/' || c.relname || '^^') || '\n' ||
            'WITH CREDENTIALS AS '|| QUOTE_LITERAL('aws_access_key_id=<aws_access_key_id>;aws_secret_access_key=<aws_secret_access_key>') || '\n' ||
            'DELIMITER AS ' || '''' || '\\t' || ''''|| '\n' ||
            'ADDQUOTES' || '\n' ||
            'ESCAPE' || '\n' ||
            'ALLOWOVERWRITE' || '\n' ||
            'GZIP;' AS ddl
        FROM pg_catalog.pg_class AS c
        LEFT JOIN pg_namespace AS n
            ON c.relnamespace = n.oid
        WHERE  n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
            AND c.relkind = 'r'

--COPY STATEMENT
        UNION
        SELECT  n.nspname AS schemaname
            , c.relname AS tablename
            , 1300000 AS seq
            , 'COPY ' || n.nspname || '.' || c.relname || '\n' ||
            'FROM ' || QUOTE_LITERAL('s3://<your_bucket/your_path>' || n.nspname || '/' || c.relname|| '^^') || '\n' ||
            'WITH CREDENTIALS AS ' || QUOTE_LITERAL('aws_access_key_id=<aws_access_key_id>;aws_secret_access_key=<aws_secret_access_key>') || '\n' ||
            'DELIMITER AS ' || '''' || '\\t' || ''''|| '\n' ||
            'DATEFORMAT '|| QUOTE_LITERAL('auto') || '\n' ||
            'TIMEFORMAT '|| QUOTE_LITERAL('auto') || '\n' ||
            'ESCAPE' || '\n' ||
            'REMOVEQUOTES ' || '\n' ||
            'GZIP' || '\n' ||
            'COMPUPDATE;' AS ddl
        FROM pg_catalog.pg_class AS c
        LEFT JOIN pg_namespace AS n
            ON c.relnamespace = n.oid
        WHERE  n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
            AND c.relkind = 'r'
            
--VIEW DDL
        UNION
        SELECT dependents.table_schemaname
            ,c.relname AS tablename
            ,1400000 AS seq
            ,('DROP VIEW IF EXISTS ' || v.schemaname || '.' || v.viewname ||';'
            || '\n CREATE OR REPLACE VIEW ' || v.schemaname || '.' || v.viewname ||' AS '
            || '\n'|| v.definition
            || '\n GRANT SELECT ON ' || v.schemaname || '.' || v.viewname || ' TO ' || CURRENT_DATABASE() || '_reporting;' 
            || '\n GRANT SELECT ON ALL TABLES IN SCHEMA ' || n.nspname || ' TO GROUP internal_users;')::VARCHAR(50000) AS ddl
        FROM(
            SELECT DISTINCT 
                srcobj.oid AS table_oid
                ,srcnsp.nspname AS table_schemaname
                ,srcobj.relname AS tablename
                ,tgtobj.oid AS dependent_viewoid
                ,tgtnsp.nspname AS schemaname
                ,tgtobj.relname AS viewname
            FROM pg_catalog.pg_class AS srcobj
            JOIN pg_catalog.pg_depend AS srcdep
                ON srcobj.oid = srcdep.refobjid
            JOIN pg_catalog.pg_depend AS tgtdep
                ON srcdep.objid = tgtdep.objid
            JOIN  pg_catalog.pg_class AS tgtobj
                ON tgtdep.refobjid = tgtobj.oid
                    AND srcobj.oid <> tgtobj.oid
            LEFT JOIN  pg_catalog.pg_namespace AS srcnsp
                ON srcobj.relnamespace = srcnsp.oid
            LEFT JOIN  pg_catalog.pg_namespace tgtnsp
                ON tgtobj.relnamespace = tgtnsp.oid
            WHERE tgtdep.deptype = 'i' 
                AND tgtobj.relkind = 'v' ) AS dependents
        JOIN pg_namespace AS n
            ON dependents.table_schemaname = n.nspname
        JOIN (
            SELECT relnamespace
                ,relname
                ,relkind
            FROM pg_class) AS c 
            ON n.oid = c.relnamespace
                AND c.relkind = 'r'
                AND dependents.tablename = c.relname
        JOIN pg_views AS v
            ON dependents.schemaname = v.schemaname
                AND dependents.viewname = v.viewname
                

--DROP DEP TABLE
        UNION
        SELECT n.nspname AS schemaname
            ,c.relname AS tablename
            ,1500000 AS seq
            ,'DROP TABLE ' || QUOTE_IDENT(n.nspname) || '.'||c.relname || '_dep_' || REPLACE(GETDATE()::DATE, '-', '_') || ';' AS ddl
        FROM pg_namespace AS n
        JOIN pg_class AS c 
            ON n.oid = c.relnamespace
        WHERE c.relkind = 'r')
       
ORDER BY schemaname
    , tablename
    , seq
 ) AS base
JOIN user_defined_values
    ON tablename = SPLIT_PART(target_data_set, '.', 2)
        AND schemaname = SPLIT_PART(target_data_set, '.', 1)
;
