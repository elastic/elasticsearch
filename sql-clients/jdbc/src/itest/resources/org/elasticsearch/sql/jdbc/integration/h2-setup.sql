DROP TABLE IF EXISTS "emp.emp";
CREATE TABLE "emp.emp" ("birth_date" TIMESTAMP WITH TIME ZONE,
                   "emp_no" INT, 
                   "first_name" VARCHAR(50), 
                   "gender" VARCHAR(1), 
                   "hire_date" TIMESTAMP WITH TIME ZONE,
                   "last_name" VARCHAR(50) 
                   )
   AS SELECT * FROM CSVREAD('classpath:/org/elasticsearch/sql/jdbc/integration/employees.csv');