DROP TABLE IF EXISTS "geo";
CREATE TABLE "geo" (
                    "city" VARCHAR(50),
                    "region" VARCHAR(50),
                    "region_point" VARCHAR(50),
                    "location" POINT,
                    "shape" GEOMETRY
)
  AS SELECT * FROM CSVREAD('classpath:/geo/geo.csv');
