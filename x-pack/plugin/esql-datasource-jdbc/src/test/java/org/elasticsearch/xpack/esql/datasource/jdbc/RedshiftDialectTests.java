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
 * Unit coverage for {@link RedshiftDialect}: the small, genuine deltas from {@link PostgresDialect} and the equally
 * important set of behaviours it deliberately inherits. The tininess of this dialect (only three overrides plus one
 * type refusal) is itself the Stage-3 thesis — Redshift is a small dialect delta on top of Postgres — so these tests
 * pin both the deltas AND the inheritance so a future change to either side is caught.
 *
 * <h2>Redshift type codes exercised (from the Redshift JDBC driver's {@code getColumns()} DATA_TYPE)</h2>
 * {@code SUPER -> -16} ({@link Types#LONGNVARCHAR}, explicitly refused here because {@link GenericDialect} would map
 * it to {@code KEYWORD}); {@code VARBYTE}/{@code GEOMETRY}/{@code GEOGRAPHY -> -4} ({@link Types#LONGVARBINARY},
 * refused via the inherited {@code default -> null}); everything unmappable {@code -> 1111} ({@link Types#OTHER},
 * also inherited-null). See {@link RedshiftDialect}'s javadoc for the driver-source citation.
 */
public class RedshiftDialectTests extends ESTestCase {

    private final RedshiftDialect dialect = RedshiftDialect.INSTANCE;

    // -- Deltas from PostgresDialect -------------------------------------------

    public void testName() {
        assertEquals("redshift", dialect.name());
    }

    public void testInitStatementsPinUtcAndDropStatementTimeout() {
        // The delta: UTC pin only, using Redshift's documented `SET timezone TO '...'` spelling, and NO
        // `SET statement_timeout` (Redshift uses WLM). Exact content + single statement.
        assertEquals(List.of("SET timezone TO 'UTC'"), dialect.initStatements());
    }

    public void testInitStatementsDifferFromPostgres() {
        // Guard the delta explicitly: Postgres keeps statement_timeout and uses the `SET TIME ZONE` spelling.
        assertEquals(List.of("SET TIME ZONE 'UTC'", "SET statement_timeout = '300000'"), PostgresDialect.INSTANCE.initStatements());
        assertNotEquals(PostgresDialect.INSTANCE.initStatements(), dialect.initStatements());
        assertFalse(
            "Redshift must not set a session statement_timeout",
            dialect.initStatements().stream().anyMatch(s -> s.toLowerCase(java.util.Locale.ROOT).contains("statement_timeout"))
        );
    }

    public void testSupportedDatabaseMajorVersionsIsEmpty() {
        // Opaque Redshift versioning -> empty set (re-widened from PostgresDialect's {12..16}) -> version WARN off.
        assertEquals(Set.of(), dialect.supportedDatabaseMajorVersions());
        assertFalse(PostgresDialect.INSTANCE.supportedDatabaseMajorVersions().isEmpty());
    }

    // -- Refused types ---------------------------------------------------------

    public void testSuperIsRefusedByExplicitOverride() {
        // SUPER reports as LONGNVARCHAR (-16). Without the override GenericDialect maps that to KEYWORD, so this
        // pins the explicit refusal AND documents the mis-mapping it prevents.
        assertNull("Redshift SUPER (LONGNVARCHAR) must be refused", dialect.mapJdbcType(Types.LONGNVARCHAR, 4194304, 0));
        assertEquals(
            "sanity: GenericDialect would otherwise map LONGNVARCHAR to KEYWORD",
            DataType.KEYWORD,
            GenericDialect.INSTANCE.mapJdbcType(Types.LONGNVARCHAR, 4194304, 0)
        );
    }

    public void testVarbyteGeometryGeographyRefusedByInheritedDefault() {
        // VARBYTE/GEOMETRY/GEOGRAPHY all report as LONGVARBINARY (-4); GenericDialect has no case, so default -> null.
        // No explicit override in RedshiftDialect: the refusal is inherited, and this test proves it.
        assertNull("VARBYTE/GEOMETRY/GEOGRAPHY (LONGVARBINARY) refused", dialect.mapJdbcType(Types.LONGVARBINARY, 0, 0));
        assertNull("inherited from GenericDialect default", GenericDialect.INSTANCE.mapJdbcType(Types.LONGVARBINARY, 0, 0));
    }

    public void testOtherIsRefusedByInheritedDefault() {
        // Anything the driver cannot describe -> OTHER (1111) -> inherited default -> null (same as Postgres).
        assertNull(dialect.mapJdbcType(Types.OTHER, 0, 0));
    }

    // -- Inherited from PostgresDialect ----------------------------------------

    public void testNumericScopingInheritedFromPostgres() {
        // The NUMERIC/DECIMAL scoping is inherited verbatim and stays inside Redshift's 38-digit envelope.
        for (int jdbcType : new int[] { Types.NUMERIC, Types.DECIMAL }) {
            assertEquals(DataType.LONG, dialect.mapJdbcType(jdbcType, 18, 0));
            assertEquals(DataType.LONG, dialect.mapJdbcType(jdbcType, 1, 0));
            assertEquals(DataType.DOUBLE, dialect.mapJdbcType(jdbcType, 19, 0));
            assertEquals(DataType.DOUBLE, dialect.mapJdbcType(jdbcType, 10, 2));
            assertEquals(DataType.DOUBLE, dialect.mapJdbcType(jdbcType, 0, 0));
            // Identical to what PostgresDialect produces -- the scoping is genuinely inherited, not re-implemented.
            assertEquals(PostgresDialect.INSTANCE.mapJdbcType(jdbcType, 18, 0), dialect.mapJdbcType(jdbcType, 18, 0));
        }
    }

    public void testCommonTypesDelegateToGeneric() {
        int[] types = {
            Types.BOOLEAN,
            Types.BIT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT,
            Types.DOUBLE,
            Types.REAL,
            Types.CHAR,
            Types.VARCHAR,
            Types.TIMESTAMP,
            Types.TIMESTAMP_WITH_TIMEZONE };
        for (int t : types) {
            assertEquals("delegation for jdbcType " + t, GenericDialect.INSTANCE.mapJdbcType(t, 10, 0), dialect.mapJdbcType(t, 10, 0));
        }
        // Concrete spot-checks so the delegation isn't vacuously "both null".
        assertEquals(DataType.BOOLEAN, dialect.mapJdbcType(Types.BOOLEAN, 0, 0));
        assertEquals(DataType.LONG, dialect.mapJdbcType(Types.BIGINT, 0, 0));
        assertEquals(DataType.KEYWORD, dialect.mapJdbcType(Types.VARCHAR, 0, 0));
        assertEquals(DataType.DATETIME, dialect.mapJdbcType(Types.TIMESTAMP_WITH_TIMEZONE, 0, 0));
    }

    public void testQuoteIdentifierInheritedAnsi() {
        assertEquals("\"col\"", dialect.quoteIdentifier("col"));
        expectThrows(IllegalArgumentException.class, () -> dialect.quoteIdentifier("a\"b"));
    }

    // -- Version discipline WARN: empty set means it NEVER warns --------------

    public void testEmptyVersionSetNeverWarns() throws Exception {
        String url = "jdbc:redshift://cluster.abc123.us-east-1.redshift.amazonaws.com:5439/db#" + randomAlphaOfLength(8);
        JdbcConnector.VERSION_WARNED.remove(url);
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "redshift never warns on version",
                    JdbcConnector.class.getName(),
                    Level.WARN,
                    "*major version*"
                )
            );
            // Even an absurd major must not warn: the empty verified set disables the check entirely.
            JdbcConnector.checkDatabaseVersion(dialect, metaDataReportingMajor(randomIntBetween(1, 99)), url);
            mockLog.assertAllExpectationsMatched();
        }
        assertFalse("empty-version dialect must not record a warned URL", JdbcConnector.VERSION_WARNED.containsKey(url));
    }

    /**
     * A {@link DatabaseMetaData} answering only {@code getDatabaseMajorVersion} (returning {@code major}); a dynamic
     * proxy is used rather than a stub subclass because {@code DatabaseMetaData} has ~200 methods (mirrors
     * {@code PostgresDialectTests}).
     */
    private static DatabaseMetaData metaDataReportingMajor(int major) {
        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "getDatabaseMajorVersion":
                    return major;
                case "toString":
                    return "DatabaseMetaData[major=" + major + "]";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                default:
                    throw new UnsupportedOperationException("unexpected DatabaseMetaData call: " + method.getName());
            }
        };
        return (DatabaseMetaData) Proxy.newProxyInstance(
            RedshiftDialectTests.class.getClassLoader(),
            new Class<?>[] { DatabaseMetaData.class },
            handler
        );
    }
}
