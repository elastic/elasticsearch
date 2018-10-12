DROP TABLE IF EXISTS "test_emp_with_nulls";
CREATE TABLE "test_emp_with_nulls" (
                    "birth_date" TIMESTAMP WITH TIME ZONE,
                    "emp_no" INT, 
                    "first_name" VARCHAR(50),
                    "gender" VARCHAR(1),
                    "hire_date" TIMESTAMP WITH TIME ZONE,
                    "languages" TINYINT,
                    "last_name" VARCHAR(50),
                    "salary" INT
                   )
   AS SELECT * FROM CSVREAD('classpath:/employees_with_nulls.csv');