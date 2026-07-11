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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class JdbcConnectorFactoryTests extends ESTestCase {

    private String jdbcUrl;
    private JdbcDriverRegistry registry;
    private JdbcConnectorFactory factory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        jdbcUrl = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        registry = JdbcDriverRegistry.fromClassLoader(getClass().getClassLoader());
        factory = new JdbcConnectorFactory(registry);
        createMixedTypeTable();
    }

    @Override
    public void tearDown() throws Exception {
        // Close the factory first: it owns the HikariCP pool built by the convenience constructor, and the pool must
        // be torn down before the registry (pooled connection teardown needs the registry's driver classloader).
        factory.close();
        registry.close();
        super.tearDown();
    }

    private void createMixedTypeTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE TABLE EMPLOYEES ("
                    + "ID INTEGER, "
                    + "NAME VARCHAR(100), "
                    + "ACTIVE BOOLEAN, "
                    + "SALARY DOUBLE, "
                    + "HIRED TIMESTAMP"
                    + ")"
            );
        }
    }

    private void createTableWithBlob() throws Exception {
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        try (Connection conn = DriverManager.getConnection(url); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE WITH_BLOB (ID INTEGER, DATA BLOB)");
        }
        jdbcUrl = url;
    }

    public void testType() {
        assertEquals("jdbc", factory.type());
    }

    public void testValidateConfigRejectsUnknownKeys() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.validateConfig(jdbcUrl, Map.of("table", "EMPLOYEES", "bogus", "x"))
        );
        assertTrue(e.getMessage().contains("unknown option"));
        assertTrue(e.getMessage().contains("bogus"));
    }

    public void testValidateConfigAcceptsClaimedKeys() {
        factory.validateConfig(
            jdbcUrl,
            Map.of(
                JdbcConnectorFactory.CONFIG_TABLE,
                "EMPLOYEES",
                JdbcConnectorFactory.CONFIG_SCHEMA,
                "PUBLIC",
                JdbcConnectorFactory.CONFIG_CATALOG,
                "CAT",
                JdbcConnectorFactory.CONFIG_USER,
                "u",
                JdbcConnectorFactory.CONFIG_PASSWORD,
                "p"
            )
        );
    }

    public void testValidateConfigAcceptsAllowlistedConnectionProperties() {
        // connection_properties is a claimed key and an allowlisted value passes validation.
        factory.validateConfig(
            jdbcUrl,
            Map.of(
                JdbcConnectorFactory.CONFIG_TABLE,
                "EMPLOYEES",
                JdbcConnectorFactory.CONFIG_CONNECTION_PROPERTIES,
                "sslmode=require;ApplicationName=es"
            )
        );
    }

    public void testValidateConfigAcceptsTypedAwsCredentialKeys() {
        // The typed AWS credential config keys are claimed, so a query supplying explicit IAM
        // credentials validates (they ride the SecureString channel; not the non-secret connection_properties map).
        factory.validateConfig(
            jdbcUrl,
            Map.of(
                JdbcConnectorFactory.CONFIG_TABLE,
                "EMPLOYEES",
                JdbcConnectorFactory.CONFIG_ACCESS_KEY_ID,
                "AKIAEXAMPLE",
                JdbcConnectorFactory.CONFIG_SECRET_ACCESS_KEY,
                "secretexample",
                JdbcConnectorFactory.CONFIG_SESSION_TOKEN,
                "tokenexample"
            )
        );
    }

    public void testValidateConfigRejectsBlockedConnectionProperty() {
        // A footgun in connection_properties fails fast at validation time (before any connect).
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.validateConfig(
                jdbcUrl,
                Map.of(
                    JdbcConnectorFactory.CONFIG_TABLE,
                    "EMPLOYEES",
                    JdbcConnectorFactory.CONFIG_CONNECTION_PROPERTIES,
                    "socketFactory=com.evil.Factory"
                )
            )
        );
        assertTrue(e.getMessage().contains("socketFactory"));
        assertTrue(e.getMessage().contains("blocked"));
    }

    public void testValidateConfigRejectsCredentialInConnectionProperties() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.validateConfig(
                jdbcUrl,
                Map.of(
                    JdbcConnectorFactory.CONFIG_TABLE,
                    "EMPLOYEES",
                    JdbcConnectorFactory.CONFIG_CONNECTION_PROPERTIES,
                    "password=hunter2"
                )
            )
        );
        assertTrue(e.getMessage().contains("credential"));
        assertFalse("must not echo the secret value", e.getMessage().contains("hunter2"));
    }

    public void testCanHandleRejectsNull() {
        assertFalse(factory.canHandle(null));
    }

    public void testCanHandleRejectsNonJdbcUrls() {
        assertFalse(factory.canHandle("http://example.com"));
        assertFalse(factory.canHandle("s3://bucket/key"));
    }

    public void testCanHandleRejectsUrlNoDriverClaims() {
        assertFalse(factory.canHandle("jdbc:nonexistent:foo"));
    }

    public void testCanHandleAcceptsH2MemUrl() {
        assertTrue(factory.canHandle(jdbcUrl));
    }

    public void testCanHandleRejectsH2FileSubprotocolViaSsrfGuard() {
        // jdbc:h2:file is NOT in the default allowlist; even though the H2 driver would accept it, the SSRF
        // guard blocks the URL before it reaches the driver. This is the primary defense against an
        // FROM "jdbc:h2:file:..." that would let the coordinator open an arbitrary file.
        assertFalse(factory.canHandle("jdbc:h2:file:/tmp/leak"));
    }

    public void testCanHandleRejectsLoopbackPostgresViaSsrfGuard() {
        // postgres is in the allowlist, but 127.0.0.1 is loopback; SSRF guard rejects.
        assertFalse(factory.canHandle("jdbc:postgresql://127.0.0.1:5432/db"));
    }

    public void testCanHandleRejectsCloudMetadataViaSsrfGuard() {
        // 169.254.169.254 is the AWS/Azure/GCP metadata IP; SSRF guard rejects link-local.
        assertFalse(factory.canHandle("jdbc:postgresql://169.254.169.254/db"));
    }

    public void testCanHandleRejectsWhenKillSwitchOff() {
        // A factory whose enabled supplier returns false treats every URL as un-handleable. The framework
        // therefore picks no connector and the query falls through to whatever default behaviour applies.
        JdbcConnectorFactory disabled = new JdbcConnectorFactory(
            registry,
            DialectRegistry.defaultRegistry(),
            SsrfGuard::defaultGuard,
            () -> false
        );
        assertFalse(disabled.canHandle(jdbcUrl));
    }

    public void testResolveMetadataRejectsWhenKillSwitchOff() {
        // DataSourceModule.LazyConnectorFactory.canHandle returns true on a scheme-prefix match alone, so the
        // resolver can reach resolveMetadata() without ever consulting our canHandle(). The guard must re-fire
        // here, otherwise a kill-switch-off connector would still open a driver connection.
        JdbcConnectorFactory disabled = new JdbcConnectorFactory(
            registry,
            DialectRegistry.defaultRegistry(),
            SsrfGuard::defaultGuard,
            () -> false
        );
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> disabled.resolveMetadata(jdbcUrl, Map.of(JdbcConnectorFactory.CONFIG_TABLE, "EMPLOYEES"))
        );
        assertTrue("expected guard rejection message, got: " + e.getMessage(), e.getMessage().contains("esql.jdbc.enabled is false"));
    }

    public void testResolveMetadataRejectsSsrfDeniedHost() {
        // Same path as the kill-switch test: lazy wrapper accepts on scheme, we get called, SSRF guard must
        // refuse before we hand the URL to driverRegistry.connect(). The H2 file subprotocol is the most
        // critical pin because the H2 driver itself accepts it and would happily read files relative to the
        // ES process CWD.
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> factory.resolveMetadata("jdbc:h2:file:/tmp/leak.db", Map.of(JdbcConnectorFactory.CONFIG_TABLE, "t"))
        );
        assertTrue("expected SSRF rejection, got: " + e.getMessage(), e.getMessage().contains("not allowed"));
    }

    public void testOpenRejectsWhenKillSwitchOff() {
        // A dynamic kill-switch flip between resolveMetadata() and open() must be honored. The resolved URL was
        // captured at planning time and is forwarded verbatim through the config map, so without the recheck
        // open() would happily build a JdbcConnector and queries would survive the flip.
        JdbcConnectorFactory disabled = new JdbcConnectorFactory(
            registry,
            DialectRegistry.defaultRegistry(),
            SsrfGuard::defaultGuard,
            () -> false
        );
        Map<String, Object> config = Map.of(JdbcConnectorFactory.RESOLVED_JDBC_URL, jdbcUrl);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> disabled.open(config));
        assertTrue("expected guard rejection, got: " + e.getMessage(), e.getMessage().contains("esql.jdbc.enabled is false"));
    }

    public void testOpenRejectsSsrfDeniedHostInResolvedConfig() {
        // Defense against a planner-time pass that resolves a benign URL but somehow stores a hostile one in
        // the resolved config. The guard at open() time still refuses.
        Map<String, Object> config = Map.of(JdbcConnectorFactory.RESOLVED_JDBC_URL, "jdbc:h2:file:/tmp/leak.db");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> factory.open(config));
        assertTrue("expected SSRF rejection, got: " + e.getMessage(), e.getMessage().contains("not allowed"));
    }

    public void testMatchPriorityJdbcPrefix() {
        assertEquals(10, factory.matchPriority(jdbcUrl));
        assertEquals(10, factory.matchPriority("jdbc:postgresql://host/db"));
    }

    public void testMatchPriorityNonJdbc() {
        assertEquals(0, factory.matchPriority("http://example.com"));
        assertEquals(0, factory.matchPriority(null));
    }

    public void testResolveMetadataMixedTypes() {
        SourceMetadata metadata = factory.resolveMetadata(jdbcUrl, Map.of(JdbcConnectorFactory.CONFIG_TABLE, "EMPLOYEES"));

        List<Attribute> schema = metadata.schema();
        assertEquals(5, schema.size());

        assertEquals("ID", schema.get(0).name());
        assertEquals(DataType.INTEGER, schema.get(0).dataType());

        assertEquals("NAME", schema.get(1).name());
        assertEquals(DataType.KEYWORD, schema.get(1).dataType());

        assertEquals("ACTIVE", schema.get(2).name());
        assertEquals(DataType.BOOLEAN, schema.get(2).dataType());

        assertEquals("SALARY", schema.get(3).name());
        assertEquals(DataType.DOUBLE, schema.get(3).dataType());

        assertEquals("HIRED", schema.get(4).name());
        assertEquals(DataType.DATETIME, schema.get(4).dataType());

        // sourceType is the COMPOUND scheme resolved from the URL (jdbc:h2 here), not the bare "jdbc" type.
        assertEquals("jdbc:h2", metadata.sourceType());
        assertEquals(jdbcUrl, metadata.location());
        assertEquals(jdbcUrl, metadata.config().get(JdbcConnectorFactory.RESOLVED_JDBC_URL));
        assertEquals("EMPLOYEES", metadata.config().get(JdbcConnectorFactory.CONFIG_TABLE));
    }

    public void testResolveMetadataSkipsUnsupportedWithWarn() throws Exception {
        createTableWithBlob();
        try (MockLog mockLog = MockLog.capture(JdbcConnectorFactory.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "skip blob column",
                    JdbcConnectorFactory.class.getCanonicalName(),
                    Level.WARN,
                    "skipping JDBC column [DATA] with unsupported type code*"
                )
            );
            SourceMetadata metadata = factory.resolveMetadata(jdbcUrl, Map.of(JdbcConnectorFactory.CONFIG_TABLE, "WITH_BLOB"));
            List<Attribute> schema = metadata.schema();
            assertEquals(1, schema.size());
            assertEquals("ID", schema.get(0).name());
            assertEquals(DataType.INTEGER, schema.get(0).dataType());
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testResolveMetadataMissingTable() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> factory.resolveMetadata(jdbcUrl, Map.of()));
        assertTrue(e.getMessage().contains("table"));
    }

    public void testOpenConstructsJdbcConnector() {
        SourceMetadata metadata = factory.resolveMetadata(jdbcUrl, Map.of(JdbcConnectorFactory.CONFIG_TABLE, "EMPLOYEES"));
        Connector connector = factory.open(metadata.config());
        assertThat(connector, instanceOf(JdbcConnector.class));
    }

    // -- LIKE-pattern hostile identifiers --

    public void testHostileTableNameWithUnderscoreDoesNotMergeColumns() throws Exception {
        // Two H2 tables: "log_2024" (literal underscore) and "logX2024" (where '_' as a LIKE wildcard would also
        // match). resolveMetadata("log_2024") must escape the underscore so only the literal table's columns are
        // returned, otherwise hostile WITH (table=...) inputs could merge schemas across tables.
        try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE \"log_2024\" (a INTEGER)");
            stmt.execute("CREATE TABLE \"logX2024\" (b VARCHAR(10), c BOOLEAN)");
        }
        SourceMetadata metadata = factory.resolveMetadata(jdbcUrl, Map.of(JdbcConnectorFactory.CONFIG_TABLE, "log_2024"));
        // Only column a must appear; b and c belong to the LIKE-matched logX2024.
        assertEquals(1, metadata.schema().size());
        assertEquals("A", metadata.schema().get(0).name().toUpperCase(java.util.Locale.ROOT));
    }

    public void testHostileTableNameWithPercentDoesNotMergeColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE \"a%b\" (x INTEGER)");
            stmt.execute("CREATE TABLE \"aXb\" (y VARCHAR(10))");
            stmt.execute("CREATE TABLE \"aSOMETHINGb\" (z BOOLEAN)");
        }
        SourceMetadata metadata = factory.resolveMetadata(jdbcUrl, Map.of(JdbcConnectorFactory.CONFIG_TABLE, "a%b"));
        assertEquals(1, metadata.schema().size());
    }

    // -- escapePattern unit --

    public void testEscapePatternEscapesUnderscoreAndPercent() {
        // Standard JDBC escape character is "\\" (backslash). H2 reports "\\" too.
        assertEquals("foo\\_bar", JdbcConnectorFactory.escapePattern("foo_bar", "\\"));
        assertEquals("foo\\%bar", JdbcConnectorFactory.escapePattern("foo%bar", "\\"));
        assertEquals("foo\\\\bar", JdbcConnectorFactory.escapePattern("foo\\bar", "\\"));
        assertEquals("plain", JdbcConnectorFactory.escapePattern("plain", "\\"));
    }

    public void testEscapePatternHandlesNullsAndEmpty() {
        assertNull(JdbcConnectorFactory.escapePattern(null, "\\"));
        assertEquals("", JdbcConnectorFactory.escapePattern("", "\\"));
        // Driver reports no escape sequence: leave the identifier alone.
        assertEquals("foo_bar", JdbcConnectorFactory.escapePattern("foo_bar", ""));
        assertEquals("foo_bar", JdbcConnectorFactory.escapePattern("foo_bar", null));
    }

    // -- SecureString hygiene: passing credentials must not corrupt the caller-owned SecureString --

    public void testResolveMetadataPreservesSecureStringBackingArray() throws Exception {
        // Set up an H2 database with credentials so we can pass a SecureString through resolveMetadata + open and
        // verify the SecureString is intact afterwards. Before the fix, asString() / openConnection() called
        // Arrays.fill on getChars()'s live backing array and corrupted the caller-owned SecureString.
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        try (Connection conn = DriverManager.getConnection(url, "sa", "topsecret"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE T (X INTEGER)");
        }
        char[] backing = "topsecret".toCharArray();
        org.elasticsearch.common.settings.SecureString pwd = new org.elasticsearch.common.settings.SecureString(backing);
        Map<String, Object> config = Map.of(
            JdbcConnectorFactory.CONFIG_TABLE,
            "T",
            JdbcConnectorFactory.CONFIG_USER,
            new org.elasticsearch.common.settings.SecureString("sa".toCharArray()),
            JdbcConnectorFactory.CONFIG_PASSWORD,
            pwd
        );
        factory.resolveMetadata(url, config);
        // The SecureString must still hold its original characters; its backing array is shared and must not be
        // zeroed by our credential plumbing.
        assertEquals("topsecret", new String(backing));
        assertEquals("topsecret", pwd.toString());
        // open() reuses the same SecureString -- exercising the second hot path that reads getChars().
        Connector connector = factory.open(factory.resolveMetadata(url, config).config());
        connector.close();
        assertEquals("topsecret", new String(backing));
    }

    // -- filterPushdownSupport() --

    public void testFilterPushdownSupportIsCachedAndNonNull() {
        // The optimizer can ask the factory many times per planning round; the support instance is stateless and
        // should be cached to avoid pointless allocations. Predicate pushdown is DEFERRED (see
        // JdbcConnectorFactory#filterPushdownSupport), but the support object is still built once and reused.
        var s1 = factory.filterPushdownSupport();
        var s2 = factory.filterPushdownSupport();
        assertNotNull(s1);
        assertSame("filterPushdownSupport must return the same instance across calls", s1, s2);
        assertThat(s1, instanceOf(JdbcFilterPushdownSupport.class));
    }

    // -- observability: INFO logs at query start + completion (projection-only path; predicate pushdown deferred) --

    public void testQueryStartLogsAtInfo() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO EMPLOYEES VALUES (1, 'a', TRUE, 1.0, NULL)");
            stmt.execute("INSERT INTO EMPLOYEES VALUES (2, 'b', FALSE, 2.0, NULL)");
        }
        SourceMetadata metadata = factory.resolveMetadata(jdbcUrl, Map.of(JdbcConnectorFactory.CONFIG_TABLE, "EMPLOYEES"));
        Connector connector = factory.open(metadata.config());
        org.elasticsearch.compute.data.BlockFactory blockFactory = org.elasticsearch.compute.data.BlockFactory.builder(
            org.elasticsearch.common.util.BigArrays.NON_RECYCLING_INSTANCE
        ).breaker(new org.elasticsearch.common.breaker.NoopCircuitBreaker("test")).build();
        QueryRequest req = new QueryRequest(
            jdbcUrl,
            List.of("ID"),
            List.of(metadata.schema().get(0)),
            metadata.config(),
            1024,
            0,
            blockFactory
        );
        try (var ml = MockLog.capture(JdbcConnector.class)) {
            ml.addExpectation(
                new MockLog.SeenEventExpectation(
                    "query start INFO log",
                    JdbcConnector.class.getName(),
                    Level.INFO,
                    "JDBC query start url=*table=[EMPLOYEES]*pushdown=[false]*"
                )
            );
            try (var cursor = connector.execute(req, (org.elasticsearch.xpack.esql.datasources.spi.Split) null)) {
                while (cursor.hasNext()) {
                    cursor.next().releaseBlocks();
                }
            }
            ml.assertAllExpectationsMatched();
        } finally {
            connector.close();
        }
    }

    public void testQueryEndLogsRowCountAndElapsed() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO EMPLOYEES VALUES (1, 'a', TRUE, 1.0, NULL)");
            stmt.execute("INSERT INTO EMPLOYEES VALUES (2, 'b', FALSE, 2.0, NULL)");
            stmt.execute("INSERT INTO EMPLOYEES VALUES (3, 'c', TRUE, 3.0, NULL)");
        }
        SourceMetadata metadata = factory.resolveMetadata(jdbcUrl, Map.of(JdbcConnectorFactory.CONFIG_TABLE, "EMPLOYEES"));
        Connector connector = factory.open(metadata.config());
        org.elasticsearch.compute.data.BlockFactory blockFactory = org.elasticsearch.compute.data.BlockFactory.builder(
            org.elasticsearch.common.util.BigArrays.NON_RECYCLING_INSTANCE
        ).breaker(new org.elasticsearch.common.breaker.NoopCircuitBreaker("test")).build();
        QueryRequest req = new QueryRequest(
            jdbcUrl,
            List.of("ID"),
            List.of(metadata.schema().get(0)),
            metadata.config(),
            1024,
            0,
            blockFactory
        );
        try (var ml = MockLog.capture(JdbcResultCursor.class)) {
            ml.addExpectation(
                new MockLog.SeenEventExpectation(
                    "query end INFO log",
                    JdbcResultCursor.class.getName(),
                    Level.INFO,
                    "JDBC query end url=*table=[EMPLOYEES] rows=[3] elapsed_ms=*pushdown=[false]"
                )
            );
            try (var cursor = connector.execute(req, (org.elasticsearch.xpack.esql.datasources.spi.Split) null)) {
                while (cursor.hasNext()) {
                    cursor.next().releaseBlocks();
                }
            }
            ml.assertAllExpectationsMatched();
        } finally {
            connector.close();
        }
    }

    // -- hostile column name in pushdown predicate --

    public void testPushedFilterRejectsColumnNameWithEmbeddedDoubleQuote() {
        // Even if a translator built a comparison with a hostile column name, the renderer (via dialect.quoteIdentifier)
        // must refuse it. Belt-and-suspenders: we never want a hostile identifier to land inside a SELECT statement.
        SqlPredicate p = new SqlPredicate.Comparison("a\"b", CompOp.EQ, new SqlParam(1, DataType.INTEGER));
        SqlRenderer renderer = new SqlRenderer(GenericDialect.INSTANCE);
        expectThrows(IllegalArgumentException.class, () -> renderer.render(p));
    }
}
