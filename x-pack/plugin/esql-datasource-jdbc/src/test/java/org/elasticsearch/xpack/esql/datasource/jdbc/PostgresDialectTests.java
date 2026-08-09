/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.apache.logging.log4j.Level;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.List;
import java.util.Set;

/**
 * Unit coverage for {@link PostgresDialect}: the {@code NUMERIC}/{@code DECIMAL} scoping matrix (the headline
 * correctness win), the exact per-connection init SQL, the observability {@link PostgresDialect#name() name} and
 * {@link PostgresDialect#supportedDatabaseMajorVersions() verified-version} metadata, delegation to
 * {@link GenericDialect} for everything else, and the once-per-URL unsupported-version WARN driven by
 * {@link JdbcConnector#checkDatabaseVersion}. No Docker: the version path uses a lightweight
 * {@link DatabaseMetaData} proxy that only answers {@code getDatabaseMajorVersion}.
 */
public class PostgresDialectTests extends ESTestCase {

    private final PostgresDialect dialect = PostgresDialect.INSTANCE;

    // -- NUMERIC / DECIMAL scoping matrix --------------------------------------

    public void testNumericScaleZeroWithinLongRangeMapsToLong() {
        // scale == 0 && 1 <= precision <= 18 -> LONG (exact big-integer keys); both NUMERIC and DECIMAL codes.
        for (int jdbcType : new int[] { Types.NUMERIC, Types.DECIMAL }) {
            assertEquals("precision 1 (jdbcType " + jdbcType + ")", DataType.LONG, dialect.mapJdbcType(jdbcType, 1, 0));
            assertEquals("precision 9 (jdbcType " + jdbcType + ")", DataType.LONG, dialect.mapJdbcType(jdbcType, 9, 0));
            assertEquals("precision 18 (jdbcType " + jdbcType + ")", DataType.LONG, dialect.mapJdbcType(jdbcType, 18, 0));
        }
    }

    public void testNumericScaleZeroBeyondLongRangeMapsToDouble() {
        // precision > 18 with scale 0 exceeds signed 64-bit range -> DOUBLE (approximate, unchanged from generic).
        for (int jdbcType : new int[] { Types.NUMERIC, Types.DECIMAL }) {
            assertEquals("precision 19 (jdbcType " + jdbcType + ")", DataType.DOUBLE, dialect.mapJdbcType(jdbcType, 19, 0));
            assertEquals("precision 38 (jdbcType " + jdbcType + ")", DataType.DOUBLE, dialect.mapJdbcType(jdbcType, 38, 0));
        }
    }

    public void testNumericWithScaleMapsToDouble() {
        // Any positive scale -> DOUBLE regardless of precision.
        for (int jdbcType : new int[] { Types.NUMERIC, Types.DECIMAL }) {
            assertEquals(DataType.DOUBLE, dialect.mapJdbcType(jdbcType, 10, 2));
            assertEquals(DataType.DOUBLE, dialect.mapJdbcType(jdbcType, 18, 1));
            assertEquals(DataType.DOUBLE, dialect.mapJdbcType(jdbcType, 5, 5));
        }
    }

    public void testNumericUnconstrainedPrecisionMapsToDouble() {
        // Unconstrained NUMERIC reports precision 0 (Postgres "numeric" with no (p,s)); not 1..18 -> DOUBLE.
        for (int jdbcType : new int[] { Types.NUMERIC, Types.DECIMAL }) {
            assertEquals(DataType.DOUBLE, dialect.mapJdbcType(jdbcType, 0, 0));
        }
    }

    public void testNonNumericTypesDelegateToGeneric() {
        // Everything outside NUMERIC/DECIMAL must map exactly as GenericDialect does (delegation via super).
        GenericDialect generic = GenericDialect.INSTANCE;
        int[] types = {
            Types.BOOLEAN,
            Types.BIT,
            Types.TINYINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT,
            Types.DOUBLE,
            Types.FLOAT,
            Types.REAL,
            Types.CHAR,
            Types.VARCHAR,
            Types.LONGVARCHAR,
            Types.DATE,
            Types.TIME,
            Types.TIMESTAMP,
            Types.TIMESTAMP_WITH_TIMEZONE,
            Types.BLOB,
            Types.ARRAY,
            Types.OTHER };
        for (int t : types) {
            assertEquals("delegation for jdbcType " + t, generic.mapJdbcType(t, 10, 0), dialect.mapJdbcType(t, 10, 0));
        }
        // Spot-check a couple of concrete expectations so the delegation isn't vacuously "both null".
        assertEquals(DataType.BOOLEAN, dialect.mapJdbcType(Types.BOOLEAN, 0, 0));
        assertEquals(DataType.LONG, dialect.mapJdbcType(Types.BIGINT, 0, 0));
        assertEquals(DataType.DATETIME, dialect.mapJdbcType(Types.TIMESTAMP_WITH_TIMEZONE, 0, 0));
        assertNull("Postgres ARRAY is refused", dialect.mapJdbcType(Types.ARRAY, 0, 0));
        assertNull("pgjdbc reports json/jsonb as OTHER -> refused", dialect.mapJdbcType(Types.OTHER, 0, 0));
    }

    // -- Metadata --------------------------------------------------------------

    public void testInitStatementsExactContentAndOrder() {
        assertEquals(List.of("SET TIME ZONE 'UTC'", "SET statement_timeout = '300000'"), dialect.initStatements());
    }

    public void testName() {
        assertEquals("postgresql", dialect.name());
    }

    public void testSupportedDatabaseMajorVersions() {
        assertEquals(Set.of(12, 13, 14, 15, 16), dialect.supportedDatabaseMajorVersions());
    }

    public void testQuoteIdentifierInheritedAnsi() {
        // Inherited from GenericDialect: ANSI double-quote, hostile input rejected.
        assertEquals("\"col\"", dialect.quoteIdentifier("col"));
        expectThrows(IllegalArgumentException.class, () -> dialect.quoteIdentifier("a\"b"));
    }

    // -- Version discipline WARN (via JdbcConnector.checkDatabaseVersion) -------

    public void testUnsupportedVersionWarnsOncePerUrl() throws Exception {
        String url = "jdbc:postgresql://host:5432/db#" + randomAlphaOfLength(8);
        JdbcConnector.VERSION_WARNED.remove(url);
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "version WARN for unsupported major 11",
                    JdbcConnector.class.getName(),
                    Level.WARN,
                    "*major version [11]*postgresql*"
                )
            );
            JdbcConnector.checkDatabaseVersion(dialect, metaDataReportingMajor(11), url);
            mockLog.assertAllExpectationsMatched();
        }
        // Second call for the same URL must NOT warn again (once-per-URL guard).
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no repeat version WARN", JdbcConnector.class.getName(), Level.WARN, "*major version*")
            );
            JdbcConnector.checkDatabaseVersion(dialect, metaDataReportingMajor(11), url);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testSupportedVersionDoesNotWarn() throws Exception {
        String url = "jdbc:postgresql://host:5432/db#" + randomAlphaOfLength(8);
        JdbcConnector.VERSION_WARNED.remove(url);
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no WARN for supported major 15",
                    JdbcConnector.class.getName(),
                    Level.WARN,
                    "*major version*"
                )
            );
            JdbcConnector.checkDatabaseVersion(dialect, metaDataReportingMajor(15), url);
            mockLog.assertAllExpectationsMatched();
        }
        assertFalse("supported version must not record a warned URL", JdbcConnector.VERSION_WARNED.containsKey(url));
    }

    public void testGenericDialectHasNoVersionDisciplineSoNeverWarns() throws Exception {
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(8);
        JdbcConnector.VERSION_WARNED.remove(url);
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("generic never warns", JdbcConnector.class.getName(), Level.WARN, "*major version*")
            );
            // Even an absurd major must not warn when the dialect declares no verified set.
            JdbcConnector.checkDatabaseVersion(GenericDialect.INSTANCE, metaDataReportingMajor(1), url);
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * A {@link DatabaseMetaData} that answers only {@code getDatabaseMajorVersion} (returning {@code major}) and
     * throws for any other method, so a test can drive {@link JdbcConnector#checkDatabaseVersion} without a real
     * connection. A dynamic proxy is used rather than a stub subclass because {@code DatabaseMetaData} has ~200
     * methods; the proxy keeps the test focused on the single method under exercise.
     */
    private static DatabaseMetaData metaDataReportingMajor(int major) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getName().equals("getDatabaseMajorVersion")) {
                return major;
            }
            if (method.getName().equals("toString")) {
                return "DatabaseMetaData[major=" + major + "]";
            }
            if (method.getName().equals("hashCode")) {
                return System.identityHashCode(proxy);
            }
            if (method.getName().equals("equals")) {
                return proxy == args[0];
            }
            throw new UnsupportedOperationException("unexpected DatabaseMetaData call: " + method.getName());
        };
        return (DatabaseMetaData) Proxy.newProxyInstance(
            PostgresDialectTests.class.getClassLoader(),
            new Class<?>[] { DatabaseMetaData.class },
            handler
        );
    }
}
