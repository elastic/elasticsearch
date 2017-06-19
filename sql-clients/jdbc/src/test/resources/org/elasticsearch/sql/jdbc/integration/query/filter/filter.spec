//
// Basic SELECT
//

wildcardWithOrder
SELECT * FROM "emp.emp" ORDER BY emp_no;
column
SELECT last_name FROM "emp.emp" ORDER BY emp_no; 
columnWithAlias
SELECT last_name AS l FROM "emp.emp" ORDER BY emp_no;
columnWithAliasNoAs
SELECT last_name l FROM "emp.emp" ORDER BY emp_no;
multipleColumnsNoAlias
SELECT first_name, last_name FROM "emp.emp" ORDER BY emp_no;
multipleColumnWithAliasWithAndWithoutAs
SELECT first_name f, last_name AS l FROM "emp.emp" ORDER BY emp_no;

//
// SELECT with LIMIT
//

wildcardWithLimit
SELECT * FROM "emp.emp" ORDER BY emp_no LIMIT 5;
wildcardWithOrderWithLimit
SELECT * FROM "emp.emp" ORDER BY emp_no LIMIT 5;
columnWithLimit
SELECT last_name FROM "emp.emp" ORDER BY emp_no LIMIT 5; 
columnWithAliasWithLimit
SELECT last_name AS l FROM "emp.emp" ORDER BY emp_no LIMIT 5;
columnWithAliasNoAsWithLimit
SELECT last_name l FROM "emp.emp" ORDER BY emp_no LIMIT 5;
multipleColumnsNoAliasWithLimit
SELECT first_name, last_name FROM "emp.emp" ORDER BY emp_no LIMIT 5;
multipleColumnWithAliasWithAndWithoutAsWithLimit
SELECT first_name f, last_name AS l FROM "emp.emp" ORDER BY emp_no LIMIT 5;


//
// SELECT with CAST
//
//castWithLiteralToInt
//SELECT CAST(1 AS INT);
castOnColumnNumberToVarchar
SELECT CAST(emp_no AS VARCHAR) AS emp_no_cast FROM "emp.emp" ORDER BY emp_no LIMIT 5;
castOnColumnNumberToLong
SELECT CAST(emp_no AS BIGINT) AS emp_no_cast FROM "emp.emp" ORDER BY emp_no LIMIT 5;
castOnColumnNumberToSmallint
SELECT CAST(emp_no AS SMALLINT) AS emp_no_cast FROM "emp.emp" ORDER BY emp_no LIMIT 5;
castOnColumnNumberWithAliasToInt
SELECT CAST(emp_no AS INT) AS emp_no_cast FROM "emp.emp" ORDER BY emp_no LIMIT 5;
castOnColumnNumberToReal
SELECT CAST(emp_no AS REAL) AS emp_no_cast FROM "emp.emp" ORDER BY emp_no LIMIT 5;
castOnColumnNumberToDouble
SELECT CAST(emp_no AS DOUBLE) AS emp_no_cast FROM "emp.emp" ORDER BY emp_no LIMIT 5;
castOnColumnNumberToBoolean
SELECT CAST(emp_no AS BOOL) AS emp_no_cast FROM "emp.emp" ORDER BY emp_no LIMIT 5;


//
// Filter 
// 

whereFieldQuality
SELECT last_name l FROM "emp.emp" WHERE emp_no = 10000 LIMIT 5;
whereFieldLessThan
SELECT last_name l FROM "emp.emp" WHERE emp_no < 10003 ORDER BY emp_no LIMIT 5;
whereFieldAndComparison
SELECT last_name l FROM "emp.emp" WHERE emp_no > 10000 AND emp_no < 10005 ORDER BY emp_no LIMIT 5;
whereFieldOrComparison
SELECT last_name l FROM "emp.emp" WHERE emp_no < 10003 OR emp_no = 10005 ORDER BY emp_no LIMIT 5;
whereFieldWithOrder
SELECT last_name l FROM "emp.emp" WHERE emp_no < 10003 ORDER BY emp_no;
whereFieldWithExactMatchOnString
SELECT last_name l FROM "emp.emp" WHERE emp_no < 10003 AND gender = 'M';
whereFieldWithLikeMatch
SELECT last_name l FROM "emp.emp" WHERE emp_no < 10003 AND last_name LIKE 'K%';
whereFieldOnMatchWithAndAndOr
SELECT last_name l FROM "emp.emp" WHERE emp_no < 10003 AND (gender = 'M' AND NOT FALSE OR last_name LIKE 'K%') ORDER BY emp_no;


//
// Group-By
//

groupByGender
SELECT gender g FROM "emp.emp" GROUP BY gender;
groupByWithWhereClause
SELECT gender g FROM "emp.emp" WHERE emp_no < 10020 GROUP BY gender;
// investigate
//groupByWithLimit
//SELECT gender g FROM "emp.emp" WHERE emp_no < 10010 GROUP BY g LIMIT 1;
