-- Postgres-NATIVE type fixture. Unlike types_matrix/employees/edge_cases this file is deliberately
-- NOT portable SQL92: it exercises vendor-specific Postgres types (NUMERIC(p,s), TIMESTAMPTZ, native TEXT,
-- SERIAL) so the gaps between the GenericDialect and a real Postgres surface empirically. It is loaded
-- ONLY by the Postgres suite (PostgresJdbcIT enables Fixture.PG_TYPES); H2 never sees it.
--
-- All values are scoped to what ES|QL can already represent (INTEGER / LONG / DOUBLE / BOOLEAN / KEYWORD /
-- DATETIME). There is deliberately NO BigDecimal / 38-digit-exact expectation, because
-- ES|QL has no arbitrary-precision decimal type. The single correctness gap this fixture demonstrates is the
-- exact-integer NUMERIC key case, everything else maps identically under both dialects.
--
-- Columns and the mapping they exercise (GenericDialect -> PostgresDialect):
--   num_key  NUMERIC(18,0)  DOUBLE (loses exactness > 2^53)  ->  LONG (exact)      <-- THE headline gap
--   num_big  NUMERIC(38,0)  DOUBLE (approximate)             ->  DOUBLE (unchanged; ES|QL has no 38-digit type)
--   num_dec  NUMERIC(10,2)  DOUBLE                           ->  DOUBLE (unchanged; scale > 0)
--   ts_tz    TIMESTAMPTZ    DATETIME                         ->  DATETIME (unchanged)
--   ts_naive TIMESTAMP      DATETIME                         ->  DATETIME (unchanged)
--   flag     BOOLEAN        BOOLEAN                          ->  BOOLEAN (unchanged)
--   txt      TEXT           KEYWORD                          ->  KEYWORD (unchanged)
--   vc       VARCHAR(32)    KEYWORD                          ->  KEYWORD (unchanged)
--   big      BIGINT         LONG                             ->  LONG (unchanged)
--   ser      SERIAL         INTEGER (INT4 at the JDBC level) ->  INTEGER (unchanged)
--
-- Value notes (chosen so the non-gap columns are exactly representable and never a false failure):
--   num_key  id 1 = 9007199254740993 = 2^53 + 1: NOT representable as a double (rounds to 2^53 = ...992), so
--            GenericDialect -> DOUBLE loses the +1; PostgresDialect -> LONG keeps it. id 2 = 2^53 (double-exact,
--            included only for a second row; not asserted for value).
--   num_big  1152921504606846976 = 2^60: a genuine > 2^53 magnitude that IS exactly representable as a double
--            (a power of two), so mapping NUMERIC(38,0) -> DOUBLE round-trips this particular value exactly. This
--            is the honest "approximate but not a type gap" case, NOT a correctness bug.
--   num_dec  12345.75: scale-2 NUMERIC; 0.75 = 3/4 is exactly representable as a double, so the DOUBLE mapping is
--            not a source of rounding fragility in the assertion.
--   ts_*     id 1 holds a KNOWN instant: ts_tz = '2020-01-02 03:04:05+00' (TIMESTAMPTZ) and the matching wall clock
--            ts_naive = '2020-01-02 03:04:05' (naive TIMESTAMP). PostgresJdbcIT#testPostgresTemporalValueReadAnchorsToUtc
--            reads the id-1 row's VALUE back through ES|QL and asserts both anchor to 2020-01-02T03:04:05Z, locking the
--            SET TIME ZONE 'UTC' initStatement together with the ColumnReader's UTC-anchored extraction.
--            The DATETIME ColumnReader materializes temporals via rs.getTimestamp(col, <Calendar in UTC>).toInstant()
--            (a driver-portable path), replacing the earlier rs.getObject(col, java.time.Instant.class) that pgjdbc
--            42.7.3 rejected -- PgResultSet.getObject(int, Class) has no Instant branch, so reading ANY Postgres temporal
--            VALUE used to throw "conversion to class java.time.Instant from <timestamptz|timestamp> not supported". The
--            shared matrix's pg_timestamp_types scenario asserts only the temporal TYPE on an EMPTY result
--            (WHERE id == -999), so ColumnReader.read is never invoked there on a materialized temporal.
CREATE TABLE pg_types (
    id INTEGER,
    num_key NUMERIC(18,0),
    num_big NUMERIC(38,0),
    num_dec NUMERIC(10,2),
    ts_tz TIMESTAMPTZ,
    ts_naive TIMESTAMP,
    flag BOOLEAN,
    txt TEXT,
    vc VARCHAR(32),
    big BIGINT,
    ser SERIAL
);

INSERT INTO pg_types (id, num_key, num_big, num_dec, ts_tz, ts_naive, flag, txt, vc, big) VALUES
    (1, 9007199254740993, 1152921504606846976, 12345.75, '2020-01-02 03:04:05+00', '2020-01-02 03:04:05', TRUE,  'hello text', 'varchar32', 9223372036854775807),
    (2, 9007199254740992, 1152921504606846976, 0.25,     '1999-12-31 23:59:59+00', '1999-12-31 23:59:59', FALSE, '',           'x',         -9223372036854775808);

-- REFUSED-TYPE table. A separate table so the pg_types matrix/value scenarios above are untouched, and so the
-- refused-type test can register an ad-hoc dataset against a table whose schema is never resolved elsewhere in the
-- suite -- a guaranteed cold schema-cache key, so the connector's cold-resolve skip WARN fires deterministically
-- regardless of test order. Every non-id column here is a Postgres type ES|QL cannot represent: the connector maps it
-- to null and SKIPS the column from the ES|QL schema (logging a WARN naming the column and the java.sql.Types code
-- pgjdbc reported), rather than crashing. The id/keep_* columns are the representable "rest of the row" that must still
-- project. The java.sql.Types code pgjdbc reports for each refused column (verified empirically by the WARN assertion
-- in PostgresJdbcIT#testRefusedColumnsAreSkipped):
--   arr  INTEGER[]  -> java.sql.Types.ARRAY (2003)   -- GenericDialect switch has no ARRAY case -> default -> null
--   js   JSON       -> java.sql.Types.OTHER (1111)   -- pgjdbc reports json / jsonb / interval / point all as OTHER
--   jsb  JSONB      -> java.sql.Types.OTHER (1111)
--   iv   INTERVAL   -> java.sql.Types.OTHER (1111)
--   pt   POINT      -> java.sql.Types.OTHER (1111)   -- native geometric type; no PostGIS extension required
-- Stored values are never projected by ES|QL (the columns are skipped); they exist only so DatabaseMetaData.getColumns
-- surfaces the columns and the "-> null (skip)" path is exercised end-to-end.
CREATE TABLE pg_refused (
    id INTEGER,
    arr INTEGER[],
    js JSON,
    jsb JSONB,
    iv INTERVAL,
    pt POINT,
    keep_txt TEXT,
    keep_num BIGINT
);

INSERT INTO pg_refused (id, arr, js, jsb, iv, pt, keep_txt, keep_num) VALUES
    (1, '{1,2,3}', '{"a":1}', '{"b":2}', '1 day',   '(1,2)', 'kept one', 111),
    (2, '{4,5}',   '{"c":3}', '{"d":4}', '2 hours', '(3,4)', 'kept two', 222);
