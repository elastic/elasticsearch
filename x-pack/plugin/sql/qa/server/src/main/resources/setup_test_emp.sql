DROP TABLE IF EXISTS "test_emp";
CREATE TABLE "test_emp" (
                    "birth_date" TIMESTAMP WITH TIME ZONE,
                    "emp_no" INT,
                    "first_name" VARCHAR(50),
                    "gender" VARCHAR(1),
                    "hire_date" TIMESTAMP WITH TIME ZONE,
                    "languages" TINYINT,
                    "last_name" VARCHAR(50),
                    "name" VARCHAR(50),
                    "salary" INT
                   )
   AS SELECT BIRTH_DATE, EMP_NO, FIRST_NAME, GENDER, HIRE_DATE, LANGUAGES, LAST_NAME, CONCAT(FIRST_NAME, ' ', LAST_NAME) AS "name", SALARY FROM CSVREAD('classpath:employees.csv');

