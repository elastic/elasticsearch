/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import java.util.List;

/**
 * Shared correctness matrix for the JDBC connector: a vendor-neutral set of ES|QL {@link Scenario scenarios} paired
 * with the exact columns and values each is expected to produce. It is the single source of truth for what "correct"
 * means, so the H2 baseline and every real-database suite (Postgres, later MySQL/Redshift) can assert
 * against the same expectations rather than re-authoring per-database checks.
 * <p>
 * <b>Parameterised by dataset name.</b> Each scenario's ES|QL is a pattern containing the {@link #DATASET_PLACEHOLDER}
 * token instead of a hard-coded {@code FROM} target. A caller resolves it via {@link Scenario#esql(String)} with
 * whatever dataset name it registered for the scenario's {@link Fixture}. This lets each database register its tables
 * under vendor-specific dataset names (e.g. {@code h2_employees} / {@code pg_employees}) while reusing one query set.
 * <p>
 * <b>Expected shapes mirror the ES|QL transport response.</b> {@link ExpectedColumn#type()} is the {@code outputType()}
 * string a column reports in {@link org.elasticsearch.xpack.core.esql.action.ColumnInfo} (e.g. {@code "integer"},
 * {@code "keyword"}); temporal columns report {@code "date"} (the {@code esType} of ES|QL {@code DATETIME}). Row cell
 * values match what {@code EsqlTestUtils.getValuesList} yields per type: {@code Integer} / {@code Long} / {@code Double}
 * / {@code Boolean} for the numeric and boolean types, and {@code String} for {@code keyword} (already UTF-8 decoded).
 * <p>
 * <b>Why temporal values are asserted by type only.</b> Scenarios exercise {@code DATE}/{@code TIMESTAMP} columns for
 * schema resolution but never assert a rendered temporal value, because cross-vendor timestamp precision and
 * timezone rendering is a dialect concern; asserting a formatted temporal string here
 * would bake H2 behaviour into the shared matrix.
 * <p>
 * This class has no dependency on the Elasticsearch test framework or any JDBC driver, so it can be lifted verbatim
 * into a {@code qa/server} module when the harness graduates to full CI.
 */
public final class JdbcTestQuerySet {

    /** Token in every {@link Scenario#esqlPattern()} replaced with the caller's dataset name by {@link Scenario#esql(String)}. */
    public static final String DATASET_PLACEHOLDER = "{dataset}";

    private JdbcTestQuerySet() {}

    /**
     * A SQL fixture under {@code src/internalClusterTest/resources/fixtures/}. Each value pairs the logical table
     * name the DDL creates with the classpath resource that creates+seeds it, and declares via {@link #portable()}
     * whether it loads unchanged on every vendor or only on a specific one.
     * <p>
     * The first three fixtures are dialect-agnostic SQL92 so they load unchanged on H2, PostgreSQL and MySQL (see the
     * header comment in each {@code .sql} file); {@link #PG_TYPES} is Postgres-native (NUMERIC/TIMESTAMPTZ/SERIAL) and
     * therefore only loaded by suites that opt into it via {@code AbstractJdbcDatabaseIT#enabledFixtures()}. A suite
     * that does not enable a fixture neither loads its DDL nor runs its scenarios.
     */
    public enum Fixture {
        /** {@code types_matrix} — one row per supported column type (INTEGER, BIGINT, DOUBLE, VARCHAR, DATE, TIMESTAMP, BOOLEAN). */
        TYPES_MATRIX("types_matrix", "/fixtures/types_matrix.sql", true),
        /** {@code employees} — 100-row realistic mixed-type dataset, consistent with the {@code JdbcDatasetIT} data. */
        EMPLOYEES("employees", "/fixtures/employees.sql", true),
        /** {@code edge_cases} — NULLs, MAX/MIN int, empty string, Unicode (emoji + CJK) and timezone-edge timestamps. */
        EDGE_CASES("edge_cases", "/fixtures/edge_cases.sql", true),
        /**
         * {@code pg_types} — Postgres-NATIVE type coverage: {@code NUMERIC(18,0)}/{@code NUMERIC(38,0)}/
         * {@code NUMERIC(10,2)}, {@code TIMESTAMPTZ}/{@code TIMESTAMP}, native {@code BOOLEAN}, {@code TEXT},
         * {@code VARCHAR}, {@code BIGINT} and {@code SERIAL}. Not portable SQL92, so only the Postgres suite enables
         * it; it surfaces the GenericDialect→PostgresDialect gaps the {@code pg_*} scenarios assert.
         */
        PG_TYPES("pg_types", "/fixtures/pg_types.sql", false),
        /**
         * {@code pushdown_parity} — an adversarial fixture (NULLs, empty string, duplicate rows, duplicate keys)
         * used exclusively by the WHERE-pushdown parity ITs ({@code AbstractJdbcPushdownParityIT}). Its DDL is
         * portable SQL92, but it is marked NON-portable so it stays OUT of the default {@link #portable() portable}
         * set: the shared correctness matrix suites (H2/Postgres) neither load it nor run scenarios against it (it
         * has none in {@link #scenarios()}), so they are unaffected. The parity ITs opt into it explicitly via
         * {@code AbstractJdbcDatabaseIT#enabledFixtures()}.
         */
        PUSHDOWN_PARITY("pushdown_parity", "/fixtures/pushdown_parity.sql", false);

        private final String tableName;
        private final String resourcePath;
        private final boolean portable;

        Fixture(String tableName, String resourcePath, boolean portable) {
            this.tableName = tableName;
            this.resourcePath = resourcePath;
            this.portable = portable;
        }

        /** Whether this fixture is portable SQL92 (loads on every vendor) or vendor-specific (opt-in per suite). */
        public boolean portable() {
            return portable;
        }

        /** The table name the fixture DDL creates (also a sensible default dataset name). */
        public String tableName() {
            return tableName;
        }

        /** Absolute classpath resource path of the DDL/DML script that creates and seeds this fixture. */
        public String resourcePath() {
            return resourcePath;
        }
    }

    /** Whether a scenario's rows must match in order, or may be compared as an unordered multiset. */
    public enum RowOrder {
        /** Rows are asserted position-by-position; the scenario's ES|QL pins order (e.g. via {@code SORT}). */
        ORDERED,
        /** Row order is not defined by the query (e.g. an unsorted filter); compare as an unordered collection. */
        UNORDERED
    }

    /**
     * One expected result column: the name ES|QL reports and its {@code outputType()} string.
     *
     * @param name the column name in the response
     * @param type the ES|QL {@code outputType()} string (e.g. {@code "integer"}, {@code "long"}, {@code "double"},
     *             {@code "keyword"}, {@code "boolean"}, {@code "date"})
     */
    public record ExpectedColumn(String name, String type) {
        public ExpectedColumn {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("column name must not be blank");
            }
            if (type == null || type.isBlank()) {
                throw new IllegalArgumentException("column type must not be blank");
            }
        }
    }

    /**
     * A single correctness scenario. Combines the parameterised ES|QL, the fixture it needs, and the expected result.
     *
     * @param queryId      unique, stable identifier used in assertion messages and to select scenarios
     * @param fixture      the SQL fixture that must be loaded for this scenario to run
     * @param esqlPattern  ES|QL text with {@link #DATASET_PLACEHOLDER} in place of the {@code FROM} target
     * @param columns      expected columns (name + type), in output order
     * @param rows         expected rows; each row's size equals {@code columns.size()}; empty for empty-result scenarios
     * @param order        whether {@code rows} is order-sensitive
     */
    public record Scenario(
        String queryId,
        Fixture fixture,
        String esqlPattern,
        List<ExpectedColumn> columns,
        List<List<Object>> rows,
        RowOrder order
    ) {
        public Scenario {
            if (queryId == null || queryId.isBlank()) {
                throw new IllegalArgumentException("queryId must not be blank");
            }
            if (fixture == null) {
                throw new IllegalArgumentException("fixture must not be null for [" + queryId + "]");
            }
            if (esqlPattern == null || esqlPattern.isBlank()) {
                throw new IllegalArgumentException("esqlPattern must not be blank for [" + queryId + "]");
            }
            if (esqlPattern.contains(DATASET_PLACEHOLDER) == false) {
                throw new IllegalArgumentException("esqlPattern for [" + queryId + "] must contain " + DATASET_PLACEHOLDER);
            }
            if (columns == null || columns.isEmpty()) {
                throw new IllegalArgumentException("columns must not be empty for [" + queryId + "]");
            }
            if (rows == null || order == null) {
                throw new IllegalArgumentException("rows and order must not be null for [" + queryId + "]");
            }
            columns = List.copyOf(columns);
            rows = List.copyOf(rows.stream().map(List::copyOf).toList());
        }

        /** Resolves {@link #esqlPattern()} against a concrete dataset name, replacing {@link #DATASET_PLACEHOLDER}. */
        public String esql(String datasetName) {
            if (datasetName == null || datasetName.isBlank()) {
                throw new IllegalArgumentException("datasetName must not be blank");
            }
            return esqlPattern.replace(DATASET_PLACEHOLDER, datasetName);
        }

        /** Expected column names, in output order. */
        public List<String> columnNames() {
            return columns.stream().map(ExpectedColumn::name).toList();
        }
    }

    // -- Expected-column type constants (ES|QL outputType strings) ----------------------------------------------------
    private static final String INTEGER = "integer";
    private static final String LONG = "long";
    private static final String DOUBLE = "double";
    private static final String KEYWORD = "keyword";
    private static final String BOOLEAN = "boolean";
    private static final String DATE = "date"; // esType of ES|QL DATETIME (what DATE/TIMESTAMP columns report)

    private static final List<Scenario> SCENARIOS = buildScenarios();

    /** The full, immutable correctness matrix. */
    public static List<Scenario> scenarios() {
        return SCENARIOS;
    }

    /** The scenarios that exercise a given fixture, in declaration order. */
    public static List<Scenario> scenariosFor(Fixture fixture) {
        return SCENARIOS.stream().filter(s -> s.fixture() == fixture).toList();
    }

    private static List<Scenario> buildScenarios() {
        return List.of(
            // -- employees: projection / LIMIT / WHERE int / keyword / IN / LIKE / STATS -------------------------------
            new Scenario(
                "emp_projection_limit",
                Fixture.EMPLOYEES,
                "FROM {dataset} | KEEP emp_no, first_name, last_name | SORT emp_no | LIMIT 3",
                List.of(
                    new ExpectedColumn("emp_no", INTEGER),
                    new ExpectedColumn("first_name", KEYWORD),
                    new ExpectedColumn("last_name", KEYWORD)
                ),
                List.of(List.of(10001, "Georgi", "Facello"), List.of(10002, "Bezalel", "Simmel"), List.of(10003, "Parto", "Bamford")),
                RowOrder.ORDERED
            ),
            new Scenario(
                "emp_limit_one",
                Fixture.EMPLOYEES,
                "FROM {dataset} | KEEP emp_no | SORT emp_no | LIMIT 1",
                List.of(new ExpectedColumn("emp_no", INTEGER)),
                List.of(List.of(10001)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "emp_where_int_eq",
                Fixture.EMPLOYEES,
                "FROM {dataset} | WHERE emp_no == 10010 | KEEP emp_no, first_name",
                List.of(new ExpectedColumn("emp_no", INTEGER), new ExpectedColumn("first_name", KEYWORD)),
                List.of(List.of(10010, "Duangkaew")),
                RowOrder.ORDERED
            ),
            new Scenario(
                "emp_where_int_gt",
                Fixture.EMPLOYEES,
                "FROM {dataset} | WHERE salary > 73000 | KEEP emp_no",
                List.of(new ExpectedColumn("emp_no", INTEGER)),
                List.of(List.of(10007), List.of(10019), List.of(10027), List.of(10029), List.of(10045), List.of(10099)),
                RowOrder.UNORDERED
            ),
            new Scenario(
                "emp_where_keyword_eq",
                Fixture.EMPLOYEES,
                "FROM {dataset} | WHERE first_name == \"Patricio\" | KEEP emp_no, last_name",
                List.of(new ExpectedColumn("emp_no", INTEGER), new ExpectedColumn("last_name", KEYWORD)),
                List.of(List.of(10012, "Bridgland")),
                RowOrder.ORDERED
            ),
            new Scenario(
                "emp_where_keyword_no_match",
                Fixture.EMPLOYEES,
                "FROM {dataset} | WHERE first_name == \"Nonexistent\" | KEEP emp_no",
                List.of(new ExpectedColumn("emp_no", INTEGER)),
                List.of(),
                RowOrder.UNORDERED
            ),
            new Scenario(
                "emp_where_in_list",
                Fixture.EMPLOYEES,
                "FROM {dataset} | WHERE emp_no IN (10001, 10002, 10003) | KEEP emp_no | SORT emp_no",
                List.of(new ExpectedColumn("emp_no", INTEGER)),
                List.of(List.of(10001), List.of(10002), List.of(10003)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "emp_where_like_prefix",
                Fixture.EMPLOYEES,
                "FROM {dataset} | WHERE last_name LIKE \"Ba*\" | KEEP emp_no, last_name | SORT emp_no",
                List.of(new ExpectedColumn("emp_no", INTEGER), new ExpectedColumn("last_name", KEYWORD)),
                List.of(List.of(10003, "Bamford"), List.of(10080, "Baek")),
                RowOrder.ORDERED
            ),
            new Scenario(
                "emp_stats_count",
                Fixture.EMPLOYEES,
                "FROM {dataset} | STATS c = COUNT(*)",
                List.of(new ExpectedColumn("c", LONG)),
                List.of(List.of(100L)),
                RowOrder.UNORDERED
            ),
            new Scenario(
                "emp_stats_min_max",
                Fixture.EMPLOYEES,
                "FROM {dataset} | STATS mx = MAX(salary), mn = MIN(salary)",
                List.of(new ExpectedColumn("mx", INTEGER), new ExpectedColumn("mn", INTEGER)),
                List.of(List.of(74999, 25324)),
                RowOrder.UNORDERED
            ),

            // -- types_matrix: multi-type projection / WHERE long / double / keyword / boolean / temporal type ---------
            new Scenario(
                "types_projection",
                Fixture.TYPES_MATRIX,
                "FROM {dataset} | KEEP id, long_val, dbl_val, str_val, bool_val | SORT id | LIMIT 2",
                List.of(
                    new ExpectedColumn("id", INTEGER),
                    new ExpectedColumn("long_val", LONG),
                    new ExpectedColumn("dbl_val", DOUBLE),
                    new ExpectedColumn("str_val", KEYWORD),
                    new ExpectedColumn("bool_val", BOOLEAN)
                ),
                List.of(List.of(1, 1000000000000L, 1.5, "alpha", true), List.of(2, 2000000000000L, 2.5, "beta", false)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "types_where_long_eq",
                Fixture.TYPES_MATRIX,
                "FROM {dataset} | WHERE long_val == 2000000000000 | KEEP id",
                List.of(new ExpectedColumn("id", INTEGER)),
                List.of(List.of(2)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "types_where_double_eq",
                Fixture.TYPES_MATRIX,
                "FROM {dataset} | WHERE dbl_val == 2.5 | KEEP id | SORT id",
                List.of(new ExpectedColumn("id", INTEGER)),
                List.of(List.of(2), List.of(4)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "types_where_keyword_eq",
                Fixture.TYPES_MATRIX,
                "FROM {dataset} | WHERE str_val == \"gamma\" | KEEP id",
                List.of(new ExpectedColumn("id", INTEGER)),
                List.of(List.of(3)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "types_where_boolean_eq",
                Fixture.TYPES_MATRIX,
                "FROM {dataset} | WHERE bool_val == true | KEEP id | SORT id",
                List.of(new ExpectedColumn("id", INTEGER)),
                List.of(List.of(1), List.of(3)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "types_temporal_columns_empty",
                Fixture.TYPES_MATRIX,
                "FROM {dataset} | WHERE id == -999 | KEEP date_val, ts_val",
                List.of(new ExpectedColumn("date_val", DATE), new ExpectedColumn("ts_val", DATE)),
                List.of(),
                RowOrder.UNORDERED
            ),

            // -- edge_cases: IS NULL / long boundary / MAX-MIN int / empty string / Unicode ----------------------------
            new Scenario(
                "edge_is_null",
                Fixture.EDGE_CASES,
                "FROM {dataset} | WHERE label IS NULL | KEEP id",
                List.of(new ExpectedColumn("id", INTEGER)),
                List.of(List.of(1)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "edge_where_long_max_value",
                Fixture.EDGE_CASES,
                "FROM {dataset} | WHERE big_val == 9223372036854775807 | KEEP id",
                List.of(new ExpectedColumn("id", INTEGER)),
                List.of(List.of(4)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "edge_max_int",
                Fixture.EDGE_CASES,
                "FROM {dataset} | WHERE id == 4 | KEEP num_val",
                List.of(new ExpectedColumn("num_val", INTEGER)),
                List.of(List.of(2147483647)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "edge_min_int",
                Fixture.EDGE_CASES,
                "FROM {dataset} | WHERE id == 5 | KEEP num_val",
                List.of(new ExpectedColumn("num_val", INTEGER)),
                List.of(List.of(-2147483648)),
                RowOrder.ORDERED
            ),
            new Scenario(
                "edge_empty_string",
                Fixture.EDGE_CASES,
                "FROM {dataset} | WHERE id == 3 | KEEP label",
                List.of(new ExpectedColumn("label", KEYWORD)),
                List.of(List.of("")),
                RowOrder.ORDERED
            ),
            new Scenario(
                "edge_unicode",
                Fixture.EDGE_CASES,
                "FROM {dataset} | WHERE id == 6 | KEEP label",
                List.of(new ExpectedColumn("label", KEYWORD)),
                List.of(List.of("😀你好")),
                RowOrder.ORDERED
            ),

            // -- pg_types (Postgres-native): NUMERIC scoping, temporal type, native boolean/text/serial ------
            //
            // Every pg_* scenario encodes the CORRECT, ESQL-scoped expectation that the PostgresDialect will
            // deliver — never a value that is wrong under the current dialect. The ONE scenario the plain
            // GenericDialect maps differently (pg_numeric_18_0_key) is listed in PostgresJdbcIT#knownGapScenarioIds so
            // it is run + recorded as an EXPECTED gap rather than weakened; the base class fails if it ever starts
            // passing under GenericDialect.

            // THE headline gap: NUMERIC(18,0) holds an exact integer > 2^53. GenericDialect maps NUMERIC -> DOUBLE and
            // the value 9007199254740993 (2^53 + 1) rounds to 9007199254740992.0, losing the +1 (and the column type
            // is DOUBLE, not LONG). PostgresDialect: scale==0 && precision<=18 -> LONG, exact. This scenario
            // asserts the LONG expectation, so under GenericDialect it fails on the column type -> known gap.
            new Scenario(
                "pg_numeric_18_0_key",
                Fixture.PG_TYPES,
                "FROM {dataset} | WHERE id == 1 | KEEP num_key",
                List.of(new ExpectedColumn("num_key", LONG)),
                List.of(List.of(9007199254740993L)),
                RowOrder.ORDERED
            ),
            // NOT a gap: NUMERIC(38,0) exceeds ES|QL's integer types, so it stays DOUBLE under BOTH dialects. The value
            // 2^60 is > 2^53 yet exactly representable as a double, so DOUBLE round-trips it exactly here — this proves
            // the mapping is DOUBLE (approximate in general) without being a false failure.
            new Scenario(
                "pg_numeric_38_0_big",
                Fixture.PG_TYPES,
                "FROM {dataset} | WHERE id == 1 | KEEP num_big",
                List.of(new ExpectedColumn("num_big", DOUBLE)),
                List.of(List.of(1152921504606846976.0)),
                RowOrder.ORDERED
            ),
            // NOT a gap: NUMERIC(10,2) has scale > 0, so DOUBLE under both dialects. 12345.75 is double-exact.
            new Scenario(
                "pg_numeric_10_2_dec",
                Fixture.PG_TYPES,
                "FROM {dataset} | WHERE id == 1 | KEEP num_dec",
                List.of(new ExpectedColumn("num_dec", DOUBLE)),
                List.of(List.of(12345.75)),
                RowOrder.ORDERED
            ),
            // NOT a gap: native Postgres BOOLEAN -> ES|QL BOOLEAN under both dialects.
            new Scenario(
                "pg_boolean_native",
                Fixture.PG_TYPES,
                "FROM {dataset} | WHERE id == 1 | KEEP flag",
                List.of(new ExpectedColumn("flag", BOOLEAN)),
                List.of(List.of(true)),
                RowOrder.ORDERED
            ),
            // NOT a gap: TEXT and VARCHAR both -> ES|QL KEYWORD under both dialects.
            new Scenario(
                "pg_text_and_varchar",
                Fixture.PG_TYPES,
                "FROM {dataset} | WHERE id == 1 | KEEP txt, vc",
                List.of(new ExpectedColumn("txt", KEYWORD), new ExpectedColumn("vc", KEYWORD)),
                List.of(List.of("hello text", "varchar32")),
                RowOrder.ORDERED
            ),
            // NOT a gap: BIGINT -> ES|QL LONG under both dialects; Long.MAX_VALUE round-trips exactly.
            new Scenario(
                "pg_bigint",
                Fixture.PG_TYPES,
                "FROM {dataset} | WHERE id == 1 | KEEP big",
                List.of(new ExpectedColumn("big", LONG)),
                List.of(List.of(9223372036854775807L)),
                RowOrder.ORDERED
            ),
            // NOT a gap: SERIAL is INT4 at the JDBC level -> ES|QL INTEGER under both dialects; the auto-assigned
            // sequence value for the first inserted row (id 1) is 1.
            new Scenario(
                "pg_serial",
                Fixture.PG_TYPES,
                "FROM {dataset} | WHERE id == 1 | KEEP ser",
                List.of(new ExpectedColumn("ser", INTEGER)),
                List.of(List.of(1)),
                RowOrder.ORDERED
            ),
            // NOT a gap (type probe): TIMESTAMPTZ and naive TIMESTAMP both -> ES|QL DATETIME ("date") under both
            // dialects. Asserted by TYPE only on an empty result, matching the shared matrix's temporal discipline
            // (rendered temporal values are a dialect concern, not asserted here). The UTC initStatement makes
            // naive-TIMESTAMP interpretation deterministic but introduces no ES|QL type/value difference to assert.
            new Scenario(
                "pg_timestamp_types",
                Fixture.PG_TYPES,
                "FROM {dataset} | WHERE id == -999 | KEEP ts_tz, ts_naive",
                List.of(new ExpectedColumn("ts_tz", DATE), new ExpectedColumn("ts_naive", DATE)),
                List.of(),
                RowOrder.UNORDERED
            )
        );
    }
}
