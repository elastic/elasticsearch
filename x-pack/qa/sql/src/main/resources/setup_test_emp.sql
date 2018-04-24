DROP TABLE IF EXISTS "test_emp";
CREATE TABLE "test_emp" (
                    "birth_date" TIMESTAMP WITH TIME ZONE,
                    "emp_no" INT, 
                    "first_name" VARCHAR(50),
                    "gender" VARCHAR(1),
                    "hire_date" TIMESTAMP WITH TIME ZONE,
                    "languages" TINYINT,
                    "last_name" VARCHAR(50),
                    "salary" INT
                   )
   AS SELECT * FROM CSVREAD('classpath:/employees.csv');