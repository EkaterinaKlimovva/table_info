WITH temp1 AS (SELECT table_name, SUBSTR(SYS_CONNECT_BY_PATH(column_name, ', '), 3) AS pthP
    FROM (SELECT table_name, column_name,
            ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY column_name) AS rn
        FROM user_constraints
        NATURAL JOIN user_cons_columns
        WHERE table_name IN (SELECT table_name FROM user_tables)
            AND constraint_type = 'P')
    WHERE CONNECT_BY_ISLEAF = 1
    START WITH rn = 1
    CONNECT BY PRIOR rn + 1 = rn AND PRIOR table_name = table_name
),
temp2 AS (SELECT table_name, SUBSTR(SYS_CONNECT_BY_PATH(column_name, ', '), 3) AS pthU
    FROM (SELECT table_name, column_name,
            ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY column_name) AS rn
        FROM user_constraints
        NATURAL JOIN user_cons_columns
        WHERE table_name IN (SELECT table_name FROM user_tables)
            AND constraint_type = 'U')
    WHERE CONNECT_BY_ISLEAF = 1
    START WITH rn = 1
    CONNECT BY PRIOR rn + 1 = rn AND PRIOR table_name = table_name
),
temp3 AS (SELECT temp1.table_name AS table_name, pthP, pthU
    FROM temp1
    FULL JOIN temp2
    ON temp1.table_name = temp2.table_name
),
temp4 AS (SELECT tpk, SUBSTR(SYS_CONNECT_BY_PATH(tfk || '(' || pcl || ')', '. '), 3) AS pth
    FROM (SELECT tpk, SUBSTR(SYS_CONNECT_BY_PATH(SUBSTR(clm, 1, 5), ', '), 3) AS pcl, tfk, rn
        FROM (SELECT tpk, tfk, 
                ROW_NUMBER() OVER (PARTITION BY tpk ORDER BY tpk) AS rn, cnfk, cnpk, clm,
                ROW_NUMBER() OVER (PARTITION BY tfk ORDER BY clm) AS rc
            FROM (SELECT cPK.table_name AS tpk, cFK.table_name AS tfk, cFK.r_constraint_name AS cnfk, cPK.constraint_name AS cnpk, user_cons_columns.column_name AS clm
                FROM user_constraints cPK
                JOIN user_constraints cFK
                ON cFK.r_constraint_name = cPK.constraint_name
                JOIN user_cons_columns
                ON cFK.constraint_name = user_cons_columns.constraint_name
                WHERE cFK.constraint_type = 'R')
            WHERE tpk != tfk)
            WHERE CONNECT_BY_ISLEAF = 1
            START WITH rc = 1
            CONNECT BY PRIOR rc + 1 = rc AND PRIOR tfk = tfk)
    WHERE CONNECT_BY_ISLEAF = 1
    START WITH rn = 1
    CONNECT BY PRIOR rn + 1 = rn AND PRIOR tpk = tpk
    ORDER BY tpk
)

SELECT table_name, PTHP, NVL(PTHU, '-'), PTH
FROM temp3
FULL JOIN temp4
ON temp3.table_name = temp4.tpk;
