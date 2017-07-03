DROP TABLE IF EXISTS "test_emp.emp";
CREATE TABLE "test_emp.emp" ("birth_date" TIMESTAMP,
                   "emp_no" INT, 
                   "first_name" VARCHAR(50), 
                   "gender" VARCHAR(1), 
                   "hire_date" TIMESTAMP,
                   "last_name" VARCHAR(50) 
                   )
   AS SELECT * FROM CSVREAD('classpath:/employees.csv');