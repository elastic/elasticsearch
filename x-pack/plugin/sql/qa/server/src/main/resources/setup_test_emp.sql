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
   
DROP TABLE IF EXISTS "logs";
CREATE TABLE "logs" (
                    "id" INT,
                    "@timestamp" TIMESTAMP WITH TIME ZONE,
                    "bytes_in" INT,
                    "bytes_out" INT,
                    "client_ip" VARCHAR(50),
                    "client_port" INT,
                    "dest_ip" VARCHAR(50),
                    "status" VARCHAR(10)
                    )
   AS SELECT * FROM CSVREAD('classpath:/logs.csv');