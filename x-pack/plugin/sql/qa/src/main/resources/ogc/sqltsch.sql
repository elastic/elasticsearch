-- FILE: sqltsch.sql 10/01/98
--
--       1         2         3         4         5         6         7         8
--345678901234567890123456789012345678901234567890123456789012345678901234567890
--//////////////////////////////////////////////////////////////////////////////
--
-- Copyright 1998, Open GIS Consortium, Inc.
--
-- The material in this document details an Open GIS Consortium Test Suite in
-- accordance with a license that your organization has signed. Please refer
-- to http://www.opengeospatial.org/testing/ to obtain a copy of the general license
-- (it is part of the Conformance Testing Agreement).
--
--//////////////////////////////////////////////////////////////////////////////
--
-- OpenGIS Simple Features for SQL (Types and Functions) Test Suite Software
--
-- This file "sqltsch.sql" is part 1 of a two part standardized test
-- suite in SQL script form. The other file that is required for this test
-- suite, "sqltque.sql", one additional script is provided ("sqltcle.sql") that
-- performs cleanup operations between test runs, and other documents that
-- describe the OGC Conformance Test Program are available via the WWW at
-- http://www.opengeospatial.org/testing/index.htm
--
-- NOTE CONCERNING INFORMATION ON CONFORMANCE TESTING AND THIS TEST SUITE
-- ----------------------------------------------------------------------
--
-- Organizations wishing to submit product for conformance testing should
-- access the above WWW site to discover the proper procedure for obtaining
-- a license to use the OpenGIS(R) certification mark associated with this
-- test suite.
--
--
-- NOTE CONCERNING TEST SUITE ADAPTATION
-- -------------------------------------
--
-- OGC recognizes that many products will have to adapt this test suite to
-- make it work properly. OGC has documented the allowable adaptations within
-- this test suite where possible. Other information about adaptations may be
-- discovered in the Test Suite Guidelines document for this test suite.
--
-- PLEASE NOTE THE OGC REQUIRES THAT ADAPTATIONS ARE FULLY DOCUMENTED USING
-- LIBERAL COMMENT BLOCKS CONFORMING TO THE FOLLOWING FORMAT:
--
-- -- !#@ ADAPTATION BEGIN
-- explanatory text goes here
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- original sql goes here
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
-- adated sql goes here
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--//////////////////////////////////////////////////////////////////////////////
--
-- BEGIN TEST SUITE CODE
--
--//////////////////////////////////////////////////////////////////////////////
--
-- Create the neccessary feature and geometry tables(views) and metadata tables
-- (views) to load and query the "Blue Lake" test data for OpenGIS Simple
-- Features for SQL (Types and Functions) test.
--
-- Required feature tables (views) are:
--                        Lakes
--                        Road Segments
--                        Divided Routes
--                        Buildings
--                        Forests
--                        Bridges
--                        Named Places
--                        Streams
--                        Ponds
--                        Map Neatlines
--
-- Please refer to the Test Suite Guidelines for this test suite for further
-- information concerning this test data.
--
--//////////////////////////////////////////////////////////////////////////////
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- CREATE SPATIAL_REF_SYS METADATA TABLE
--
--//////////////////////////////////////////////////////////////////////////////
--
--
-- *** ADAPTATION ALERT ****
-- Implementations do not need to execute this statement if they already
-- create the spatial_ref_sys table or view via another mechanism.
-- The size of the srtext VARCHAR exceeds that allowed on some systems.
--
-- CREATE TABLE spatial_ref_sys (
--        srid       INTEGER NOT NULL PRIMARY KEY,
--        auth_name  VARCHAR(256),
--        auth_srid  INTEGER,
-- --       srtext     VARCHAR(2048)
--        srtext     VARCHAR(2000)
-- );
-- --
-- INSERT INTO spatial_ref_sys VALUES(101, 'POSC', 32214,
-- 'PROJCS["UTM_ZONE_14N", GEOGCS["World Geodetic System 72",
-- DATUM["WGS_72",  SPHEROID["NWL_10D", 6378135, 298.26]],
-- PRIMEM["Greenwich", 0], UNIT["Meter", 1.0]],
-- PROJECTION["Transverse_Mercator"],
-- PARAMETER["False_Easting", 500000.0],
-- PARAMETER["False_Northing", 0.0],
-- PARAMETER["Central_Meridian", -99.0],
-- PARAMETER["Scale_Factor", 0.9996],
-- PARAMETER["Latitude_of_origin", 0.0],
-- UNIT["Meter", 1.0]]'
-- );
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
--                        CREATE FEATURE SCHEMA
--
-- *** ADAPTATION ALERT ***
-- The following schema is created using CREATE TABLE statements.
-- Furthermore, it DOES NOT create the GEOMETRY_COLUMNS metadata table.
-- Implementer's should replace the CREATE TABLES below with the mechanism
-- that it uses to create feature tables and the GEOMETRY_COLUMNS table/view
--
--//////////////////////////////////////////////////////////////////////////////
--
--------------------------------------------------------------------------------
--
-- Create feature tables
--
--------------------------------------------------------------------------------
--
-- Lakes
--
--
--
--
CREATE TABLE lakes (
       fid               INTEGER NOT NULL PRIMARY KEY,
       name              VARCHAR(64),
       shore             POLYGON
);
--
-- Road Segments
--
--
--
--
CREATE TABLE road_segments (
       fid               INTEGER NOT NULL PRIMARY KEY,
       name              VARCHAR(64),
       aliases           VARCHAR(64),
       num_lanes         INTEGER,
       centerline        LINESTRING
);
--
-- Divided Routes
--
--
--
--
CREATE TABLE divided_routes (
       fid               INTEGER NOT NULL PRIMARY KEY,
       name              VARCHAR(64),
       num_lanes         INTEGER,
       centerlines       MULTILINESTRING
);
--
-- Forests
--
--
--
--
CREATE TABLE forests (
       fid            INTEGER NOT NULL PRIMARY KEY,
       name           VARCHAR(64),
       boundary       MULTIPOLYGON
);
--
-- Bridges
--
--
--
--
CREATE TABLE bridges (
       fid           INTEGER NOT NULL PRIMARY KEY,
       name          VARCHAR(64),
       position      POINT
);
--
-- Streams
--
--
--
--
CREATE TABLE streams (
       fid             INTEGER NOT NULL PRIMARY KEY,
       name            VARCHAR(64),
       centerline      LINESTRING
);
--
-- Buildings
--
--*** ADAPTATION ALERT ***
-- A view could be used to provide the below semantics without multiple geometry
-- columns in a table. In other words, create two tables. One table would
-- contain the POINT position and the other would create the POLYGON footprint.
-- Then create a view with the semantics of the buildings table below.
--
--
--
CREATE TABLE buildings (
       fid             INTEGER NOT NULL PRIMARY KEY,
       address         VARCHAR(64),
       position        POINT,
       footprint       POLYGON
);
--
-- Ponds
--
--
--
--
-- -- !#@ ADAPTATION BEGIN
-- Fixes typo in the MULTIPOYLGON type
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- CREATE TABLE ponds (
--        fid             INTEGER NOT NULL PRIMARY KEY,
--        name            VARCHAR(64),
--        type            VARCHAR(64),
--        shores          MULTIPOYLGON
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
CREATE TABLE ponds (
       fid             INTEGER NOT NULL PRIMARY KEY,
       name            VARCHAR(64),
       type            VARCHAR(64),
       shores          MULTIPOLYGON
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END

--
-- Named Places
--
--
--
--
CREATE TABLE named_places (
       fid             INTEGER NOT NULL PRIMARY KEY,
       name            VARCHAR(64),
       boundary        POLYGON
);
--
-- Map Neatline
--
--
--
--
CREATE TABLE map_neatlines (
       fid             INTEGER NOT NULL PRIMARY KEY,
       neatline        POLYGON
);
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- POPULATE GEOMETRY AND FEATURE TABLES
--
-- *** ADAPTATION ALERT ***
-- This script DOES NOT make any inserts into a GEOMTERY_COLUMNS table/view.
-- Implementers should insert whatever makes this happen in their implementation
-- below. Furthermore, the inserts below may be replaced by whatever mechanism
-- may be provided by implementers to insert rows in feature tables such that
-- metadata (and other mechanisms) are updated properly.
--
--//////////////////////////////////////////////////////////////////////////////
--
--==============================================================================
-- Lakes
--
-- We have one lake, Blue Lake. It is a polygon with a hole. Its geometry is
-- described in WKT format as:
--   'POLYGON( (52 18, 66 23, 73  9, 48  6, 52 18),
--             (59 18, 67 18, 67 13, 59 13, 59 18) )'
--==============================================================================
--
--
-- -- !#@ ADAPTATION BEGIN
-- Adds ST_ prefix to routing names
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- INSERT INTO lakes VALUES (101, 'BLUE LAKE',
--     PolygonFromText('POLYGON((52 18,66 23,73 9,48 6,52 18),(59 18,67 18,67 13,59 13,59 18))', 101)
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
INSERT INTO lakes VALUES (101, 'BLUE LAKE',
    ST_PolyFromText('POLYGON((52 18,66 23,73 9,48 6,52 18),(59 18,67 18,67 13,59 13,59 18))', 101)
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--==================
-- Road segments
--
-- We have five road segments. Their geometries are all linestrings.
-- The geometries are described in WKT format as:
--  name 'Route 5', fid 102
--   'LINESTRING( 0 18, 10 21, 16 23, 28 26, 44 31 )'
--  name 'Route 5', fid 103
--   'LINESTRING( 44 31, 56 34, 70 38 )'
--  name 'Route 5', fid 104
--   'LINESTRING( 70 38, 72 48 )'
--  name 'Main Street', fid 105
--   'LINESTRING( 70 38, 84 42 )'
--  name 'Dirt Road by Green Forest', fid 106
--   'LINESTRING( 28 26, 28 0 )'
--
--==================
--
--
-- -- !#@ ADAPTATION BEGIN
-- Adds ST_ prefix to routing names
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- INSERT INTO road_segments VALUES(102, 'Route 5', NULL, 2,
--     LineStringFromText('LINESTRING( 0 18, 10 21, 16 23, 28 26, 44 31 )' ,101)
-- );
-- INSERT INTO road_segments VALUES(103, 'Route 5', 'Main Street', 4,
--     LineStringFromText('LINESTRING( 44 31, 56 34, 70 38 )' ,101)
-- );
-- INSERT INTO road_segments VALUES(104, 'Route 5', NULL, 2,
--     LineStringFromText('LINESTRING( 70 38, 72 48 )' ,101)
-- );
-- INSERT INTO road_segments VALUES(105, 'Main Street', NULL, 4,
--     LineStringFromText('LINESTRING( 70 38, 84 42 )' ,101)
-- );
-- INSERT INTO road_segments VALUES(106, 'Dirt Road by Green Forest', NULL, 1,
--     LineStringFromText('LINESTRING( 28 26, 28 0 )',101)
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
INSERT INTO road_segments VALUES(102, 'Route 5', NULL, 2,
    ST_LineFromText('LINESTRING( 0 18, 10 21, 16 23, 28 26, 44 31 )' ,101)
);
INSERT INTO road_segments VALUES(103, 'Route 5', 'Main Street', 4,
    ST_LineFromText('LINESTRING( 44 31, 56 34, 70 38 )' ,101)
);
INSERT INTO road_segments VALUES(104, 'Route 5', NULL, 2,
    ST_LineFromText('LINESTRING( 70 38, 72 48 )' ,101)
);
INSERT INTO road_segments VALUES(105, 'Main Street', NULL, 4,
    ST_LineFromText('LINESTRING( 70 38, 84 42 )' ,101)
);
INSERT INTO road_segments VALUES(106, 'Dirt Road by Green Forest', NULL, 1,
    ST_LineFromText('LINESTRING( 28 26, 28 0 )',101)
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END

--
--==================
-- DividedRoutes
--
-- We have one divided route. Its geometry is a multilinestring.
-- The geometry is described in WKT format as:
--   'MULTILINESTRING( (10 48, 10 21, 10 0), (16 0, 10 23, 16 48) )'
--
--==================
--
-- -- !#@ ADAPTATION BEGIN
-- Adds ST_ prefix to routing names
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- INSERT INTO divided_routes VALUES(119, 'Route 75', 4,
--     MultiLineStringFromText('MULTILINESTRING((10 48,10 21,10 0),(16 0,16 23,16 48))', 101)
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
INSERT INTO divided_routes VALUES(119, 'Route 75', 4,
    ST_MLineFromText('MULTILINESTRING((10 48,10 21,10 0),(16 0,16 23,16 48))', 101)
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--==================
-- Forests
--
-- We have one forest. Its geometry is a multipolygon.
-- The geometry is described in WKT format as:
--   'MULTIPOLYGON( ( (28 26, 28 0, 84 0, 84 42, 28 26),
--                    (52 18, 66 23, 73 9, 48 6, 52 18) ),
--                  ( (59 18, 67 18, 67 13, 59 13, 59 18) ) )'
--
--==================
--
-- -- !#@ ADAPTATION BEGIN
-- Adds ST_ prefix to routing names
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- INSERT INTO forests VALUES(109, 'Green Forest',
--     MultiPolygonFromText('MULTIPOLYGON(((28 26,28 0,84 0,84 42,28 26),(52 18,66 23,73 9,48 6,52 18)),((59 18,67 18,67 13,59 13,59 18)))', 101)
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
INSERT INTO forests VALUES(109, 'Green Forest',
    ST_MPolyFromText('MULTIPOLYGON(((28 26,28 0,84 0,84 42,28 26),(52 18,66 23,73 9,48 6,52 18)),((59 18,67 18,67 13,59 13,59 18)))', 101)
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END

--
--==================
-- Bridges
--
-- We have one bridge. Its geometry is a point.
-- The geometry is described in WKT format as:
--   'POINT( 44 31 )'
--
--==================
--
-- -- !#@ ADAPTATION BEGIN
-- Adds ST_ prefix to routing names
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- INSERT INTO bridges VALUES(110, 'Cam Bridge',
--     PointFromText('POINT( 44 31 )', 101)
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
INSERT INTO bridges VALUES(110, 'Cam Bridge',
    ST_PointFromText('POINT( 44 31 )', 101)
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--==================
-- Streams
--
-- We have two streams. Their geometries are linestrings.
-- The geometries are described in WKT format as:
--   'LINESTRING( 38 48, 44 41, 41 36, 44 31, 52 18 )'
--   'LINESTRING( 76 0, 78 4, 73 9 )'
--
--==================
--
-- -- !#@ ADAPTATION BEGIN
-- Adds ST_ prefix to routing names
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- INSERT INTO streams VALUES(111, 'Cam Stream',
--     LineStringFromText('LINESTRING( 38 48, 44 41, 41 36, 44 31, 52 18 )', 101)
-- );
-- INSERT INTO streams VALUES(112, NULL,
--     LineStringFromText('LINESTRING( 76 0, 78 4, 73 9 )', 101)
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
INSERT INTO streams VALUES(111, 'Cam Stream',
    ST_LineFromText('LINESTRING( 38 48, 44 41, 41 36, 44 31, 52 18 )', 101)
);
INSERT INTO streams VALUES(112, NULL,
    ST_LineFromText('LINESTRING( 76 0, 78 4, 73 9 )', 101)
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--==================
-- Buildings
--
-- We have two buildings. Their geometries are points and polygons.
-- The geometries are described in WKT format as:
--  address '123 Main Street' fid 113
--   'POINT( 52 30 )' and
--   'POLYGON( ( 50 31, 54 31, 54 29, 50 29, 50 31) )'
--  address '215 Main Street' fid 114
--   'POINT( 64 33 )' and
--   'POLYGON( ( 66 34, 62 34, 62 32, 66 32, 66 34) )'
--
--==================
--
-- -- !#@ ADAPTATION BEGIN
-- Adds ST_ prefix to routing names
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- INSERT INTO buildings VALUES(113, '123 Main Street',
--     PointFromText('POINT( 52 30 )', 101),
--     PolygonFromText('POLYGON( ( 50 31, 54 31, 54 29, 50 29, 50 31) )', 101)
-- );
-- INSERT INTO buildings VALUES(114, '215 Main Street',
--     PointFromText('POINT( 64 33 )', 101),
--     PolygonFromText('POLYGON( ( 66 34, 62 34, 62 32, 66 32, 66 34) )', 101)
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
INSERT INTO buildings VALUES(113, '123 Main Street',
    ST_PointFromText('POINT( 52 30 )', 101),
    ST_PolyFromText('POLYGON( ( 50 31, 54 31, 54 29, 50 29, 50 31) )', 101)
);
INSERT INTO buildings VALUES(114, '215 Main Street',
    ST_PointFromText('POINT( 64 33 )', 101),
    ST_PolyFromText('POLYGON( ( 66 34, 62 34, 62 32, 66 32, 66 34) )', 101)
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--==================
-- Ponds
--
-- We have one pond. Its geometry is a multipolygon.
-- The geometry is described in WKT format as:
--   'MULTIPOLYGON( ( ( 24 44, 22 42, 24 40, 24 44) ), ( ( 26 44, 26 40, 28 42, 26 44) ) )'
--
--==================
--
-- -- !#@ ADAPTATION BEGIN
-- Adds ST_ prefix to routing names
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- INSERT INTO ponds VALUES(120, NULL, 'Stock Pond',
--     MultiPolygonFromText('MULTIPOLYGON( ( ( 24 44, 22 42, 24 40, 24 44) ), ( ( 26 44, 26 40, 28 42, 26 44) ) )', 101)
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
INSERT INTO ponds VALUES(120, NULL, 'Stock Pond',
    ST_MPolyFromText('MULTIPOLYGON( ( ( 24 44, 22 42, 24 40, 24 44) ), ( ( 26 44, 26 40, 28 42, 26 44) ) )', 101)
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END

--
--==================
-- Named Places
--
-- We have two named places. Their geometries are polygons.
-- The geometries are described in WKT format as:
--  name 'Ashton' fid 117
--   'POLYGON( ( 62 48, 84 48, 84 30, 56 30, 56 34, 62 48) )'
--  address 'Goose Island' fid 118
--   'POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )'
--
--==================
--
-- -- !#@ ADAPTATION BEGIN
-- Adds ST_ prefix to routing names
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- INSERT INTO named_places VALUES(117, 'Ashton',
--     PolygonFromText('POLYGON( ( 62 48, 84 48, 84 30, 56 30, 56 34, 62 48) )', 101)
-- );
-- INSERT INTO named_places VALUES(118, 'Goose Island',
--     PolygonFromText('POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )', 101)
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
INSERT INTO named_places VALUES(117, 'Ashton',
    ST_PolyFromText('POLYGON( ( 62 48, 84 48, 84 30, 56 30, 56 34, 62 48) )', 101)
);
INSERT INTO named_places VALUES(118, 'Goose Island',
    ST_PolyFromText('POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )', 101)
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--==================
-- Map Neatlines
--
-- We have one map neatline. Its geometry is a polygon.
-- The geometry is described in WKT format as:
--   'POLYGON( ( 0 0, 0 48, 84 48, 84 0, 0 0 ) )'
--
--==================
--
-- -- !#@ ADAPTATION BEGIN
-- Adds ST_ prefix to routing names
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- INSERT INTO map_neatlines VALUES(115,
--     PolygonFromText('POLYGON( ( 0 0, 0 48, 84 48, 84 0, 0 0 ) )', 101)
-- );
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
INSERT INTO map_neatlines VALUES(115,
    ST_PolyFromText('POLYGON( ( 0 0, 0 48, 84 48, 84 0, 0 0 ) )', 101)
);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--
--
-- end sqltsch.sql