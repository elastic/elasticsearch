//
// DateTime
//

//
// Time
//
dateTimeSecond
SELECT SECOND(birth_date) d, last_name l FROM "emp.emp" WHERE emp_no < 10010 ORDER BY emp_no;
dateTimeMinute
SELECT MINUTE(birth_date) d, last_name l FROM "emp.emp" WHERE emp_no < 10010 ORDER BY emp_no;
dateTimeHour
SELECT HOUR(birth_date) d, last_name l FROM "emp.emp" WHERE emp_no < 10010 ORDER BY emp_no;


//
// Date
//

dateTimeDay
SELECT DAY(birth_date) d, last_name l FROM "emp.emp" WHERE emp_no < 10010 ORDER BY emp_no;
dateTimeDayOfMonth
SELECT DAY_OF_MONTH(birth_date) d, last_name l FROM "emp.emp" WHERE emp_no < 10010 ORDER BY emp_no;
// dateTimeDayOfWeek - disable since it starts at 0 not 1
// SELECT DAY_OF_WEEK(birth_date) d, last_name l FROM "emp.emp" WHERE emp_no < 10010 ORDER BY emp_no;
dateTimeDayOfYear
SELECT DAY_OF_YEAR(birth_date) d, last_name l FROM "emp.emp" WHERE emp_no < 10010 ORDER BY emp_no;
dateTimeMonth
SELECT MONTH(birth_date) d, last_name l FROM "emp.emp" WHERE emp_no < 10010 ORDER BY emp_no;
dateTimeYear
SELECT YEAR(birth_date) d, last_name l FROM "emp.emp" WHERE emp_no < 10010 ORDER BY emp_no;

// NOCOMMIT figure out a way to test the aliases that are unsupported by h2 but are supported by us because they are like PostgreSQL


//
// Filter
//
dateTimeFilterDayOfMonth
SELECT DAY_OF_MONTH(birth_date) AS d, last_name l FROM "emp.emp" WHERE DAY_OF_MONTH(birth_date) <= 10 ORDER BY emp_no LIMIT 5;
dateTimeFilterMonth
SELECT MONTH(birth_date) AS d, last_name l FROM "emp.emp" WHERE MONTH(birth_date) <= 5 ORDER BY emp_no LIMIT 5;
dateTimeFilterYear
SELECT YEAR(birth_date) AS d, last_name l FROM "emp.emp" WHERE YEAR(birth_date) <= 1960 ORDER BY emp_no LIMIT 5;

//
// Aggregate
//
dateTimeAggByYear
SELECT YEAR(birth_date) AS d, CAST(SUM(emp_no) AS INT) s FROM "emp.emp" GROUP BY YEAR(birth_date) ORDER BY YEAR(birth_date) LIMIT 5;
// NOCOMMIT Elasticsearch doesn't line up
// dateTimeAggByMonth
// SELECT MONTH(birth_date) AS d, CAST(SUM(emp_no) AS INT) s FROM "emp.emp" GROUP BY MONTH(birth_date) ORDER BY MONTH(birth_date) LIMIT 5;
// dateTimeAggByDayOfMonth
// SELECT DAY_OF_MONTH(birth_date) AS d, CAST(SUM(emp_no) AS INT) s FROM "emp.emp" GROUP BY DAY_OF_MONTH(birth_date) ORDER BY DAY_OF_MONTH(birth_date) DESC;
