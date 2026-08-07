/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Retry policy for {@link JdbcSqlStateCategory#TRANSIENT_NETWORK} (and the "everything else fails fast" contract) in
 * {@link JdbcConnector}, exercised through the real {@code execute()} path with a scripted
 * {@link JdbcConnector.ConnectionSource} standing in for the HikariCP pool (no Docker, no pool threads). The success
 * attempt hands back a real in-process H2 connection so {@code prepareStatement}/{@code executeQuery} run for real.
 * <p>
 * Key distinction pinned here: a HikariCP pool-acquisition timeout (already translated to an
 * {@link IllegalStateException} by the pool) is NOT a driver {@code TRANSIENT_NETWORK} SQLException and must NOT be
 * retried — pool exhaustion is not a network blip.
 */
public class JdbcTransientRetryTests extends ESTestCase {

    private BlockFactory blockFactory;
    private String jdbcUrl;
    private Connection keepAlive;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST))
            .build();
        jdbcUrl = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        // Keep-alive so the in-mem DB (and its single-row table T) survives across borrows for the query's lifetime.
        keepAlive = DriverManager.getConnection(jdbcUrl);
        try (var st = keepAlive.createStatement()) {
            st.execute("CREATE TABLE T (A INTEGER)");
            st.execute("INSERT INTO T VALUES (1)");
        }
    }

    @Override
    public void tearDown() throws Exception {
        keepAlive.close();
        super.tearDown();
    }

    public void testTransientNetworkRetriedOnceThenSucceeds() throws Exception {
        // 08000 (connection_exception) on the first borrow, a real connection on the second: exactly one retry.
        ScriptedSource source = new ScriptedSource(1, () -> new SQLException("connection dropped", "08000"));
        JdbcConnector connector = connector(source);
        try (ResultCursor cursor = connector.execute(request(), (Split) null)) {
            assertNotNull(cursor);
        }
        assertEquals("exactly one retry (2 borrow attempts)", 2, source.calls.get());
    }

    public void testTransientRetryIsExactlyOnceThenPropagates() throws Exception {
        // 08000 on BOTH borrows: retried exactly once, then the failure propagates (no unbounded loop).
        ScriptedSource source = new ScriptedSource(2, () -> new SQLException("still down", "08000"));
        JdbcConnector connector = connector(source);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> connector.execute(request(), (Split) null));
        assertEquals("initial attempt + exactly one retry", 2, source.calls.get());
        assertTrue("message carries SQLState: " + e.getMessage(), e.getMessage().contains("sqlstate=08000"));
        assertTrue("message carries category: " + e.getMessage(), e.getMessage().contains("category=[TRANSIENT_NETWORK]"));
        // Sanitized URL only -- no raw driver text.
        assertTrue(e.getMessage().contains("failed to execute JDBC query against"));
    }

    public void testPoolTimeoutIllegalStateIsNotRetried() throws Exception {
        // Model the pool's translation: JdbcHikariPool converts a HikariCP SQLTransientConnectionException (pool
        // acquisition timeout) into an IllegalStateException BEFORE returning to the connector. Because it is not a
        // SQLException, the classifier-driven retry never sees it: it must propagate on the FIRST attempt.
        ScriptedSource source = new ScriptedSource(0, null) {
            @Override
            public Connection getConnection(String url, Properties props) {
                calls.incrementAndGet();
                throw new IllegalStateException(
                    "no JDBC connection available within 5000ms; target=[" + jdbcUrl + "] pool_max=10 in_use=10"
                );
            }
        };
        JdbcConnector connector = connector(source);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> connector.execute(request(), (Split) null));
        assertEquals("pool timeout must NOT be retried", 1, source.calls.get());
        assertTrue(
            "pool-timeout message must pass through unchanged: " + e.getMessage(),
            e.getMessage().contains("no JDBC connection available within 5000ms")
        );
    }

    public void testHikariTransientConnectionExceptionClassifiesAsUnknownNotTransient() {
        // Belt-and-suspenders: even a raw HikariCP SQLTransientConnectionException (no SQLState) must classify as
        // UNKNOWN, never TRANSIENT_NETWORK -- so pool exhaustion can never masquerade as a retryable network blip.
        SQLTransientConnectionException hikariTimeout = new SQLTransientConnectionException(
            "esql-jdbc[...] - Connection is not available, request timed out after 5000ms"
        );
        assertEquals(JdbcSqlStateCategory.UNKNOWN, JdbcSqlStateClassifier.classify(hikariTimeout));
    }

    public void testNonRetryableCategoriesPropagateOnFirstAttempt() throws Exception {
        // One representative SQLState per non-retryable category: each must propagate WITHOUT a retry.
        Map<String, String> stateToCategory = Map.of(
            "42601",
            "SYNTAX_ERROR",
            "22P02",
            "DATA_ERROR",
            "42501",
            "PERMISSION",
            "40001",
            "DEADLOCK",
            "53300",
            "RESOURCE_EXHAUSTED",
            "23505",
            "INTEGRITY_VIOLATION",
            "57014",
            "CANCELLED_BY_USER",
            "08P01",
            "UNKNOWN", // protocol violation: connection-class but deliberately non-retryable
            "99999",
            "UNKNOWN"
        );
        for (Map.Entry<String, String> entry : stateToCategory.entrySet()) {
            String state = entry.getKey();
            ScriptedSource source = new ScriptedSource(1, () -> new SQLException("boom", state));
            JdbcConnector connector = connector(source);
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> connector.execute(request(), (Split) null));
            assertEquals("SQLState [" + state + "] must not be retried", 1, source.calls.get());
            assertTrue(
                "message must carry category [" + entry.getValue() + "] for SQLState [" + state + "]: " + e.getMessage(),
                e.getMessage().contains("category=[" + entry.getValue() + "]")
            );
        }
    }

    public void testTransientRetryWorksIndependentlyOfCredentialRefresh() throws Exception {
        // Transient retry does not depend on credential refreshability: a non-refreshable (per-query) credential
        // source still gets its single TRANSIENT_NETWORK retry. This keeps the transient path distinct from the
        // AUTH_FAILED credential-refresh path.
        ScriptedSource source = new ScriptedSource(1, () -> new SQLException("blip", "08006"));
        JdbcConnector connector = new JdbcConnector(
            source,
            GenericDialect.INSTANCE,
            jdbcUrl,
            nonRefreshableCredentials(),
            () -> 0L // epoch never changes; irrelevant to the transient path
        );
        try (ResultCursor cursor = connector.execute(request(), (Split) null)) {
            assertNotNull(cursor);
        }
        assertEquals(2, source.calls.get());
    }

    // -- helpers -------------------------------------------------------------------------------

    private JdbcConnector connector(JdbcConnector.ConnectionSource source) {
        return new JdbcConnector(source, GenericDialect.INSTANCE, jdbcUrl, nonRefreshableCredentials(), () -> 0L);
    }

    private static JdbcConnector.CredentialSource nonRefreshableCredentials() {
        return new JdbcConnector.CredentialSource() {
            @Override
            public void writeInto(Properties props) {
                // no credentials needed for H2 in-mem
            }

            @Override
            public boolean refreshable() {
                return false;
            }
        };
    }

    private QueryRequest request() {
        Attribute a = new FieldAttribute(
            Source.EMPTY,
            "A",
            new EsField("A", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.UNKNOWN)
        );
        return new QueryRequest("t", List.of("A"), List.of(a), Map.of("table", "T"), 1024, 0, blockFactory);
    }

    /**
     * A {@link JdbcConnector.ConnectionSource} that fails its first {@code failCount} borrows with a scripted
     * {@link SQLException} and then hands back a real in-process H2 connection. {@code calls} records every borrow so
     * a test can assert the exact retry count.
     */
    private class ScriptedSource implements JdbcConnector.ConnectionSource {
        final AtomicInteger calls = new AtomicInteger();
        private final int failCount;
        private final java.util.function.Supplier<SQLException> failure;

        ScriptedSource(int failCount, java.util.function.Supplier<SQLException> failure) {
            this.failCount = failCount;
            this.failure = failure;
        }

        @Override
        public Connection getConnection(String url, Properties props) throws SQLException {
            int n = calls.incrementAndGet();
            if (n <= failCount) {
                throw failure.get();
            }
            return DriverManager.getConnection(jdbcUrl);
        }
    }
}
