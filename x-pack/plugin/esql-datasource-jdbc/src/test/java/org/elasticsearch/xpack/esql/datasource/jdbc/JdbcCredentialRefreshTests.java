/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Credential-refresh retry policy for {@link JdbcSqlStateCategory#AUTH_FAILED} in {@link JdbcConnector}, plus the
 * {@link JdbcDataSourcePlugin#reload} epoch seam.
 * <p>
 * <b>Design note.</b> Credentials are currently per-query
 * {@link org.elasticsearch.common.settings.SecureString}s decrypted from the data-source definition; there is NO
 * node-keystore credential source, so per-query credentials are not refreshable. The honest behavior is therefore
 * that an {@code AUTH_FAILED} against a per-query (non-refreshable) source fails fast — re-reading byte-identical
 * credentials would be a fake refresh. The retry mechanism itself IS real and epoch-gated, so a future
 * keystore-backed (refreshable) source plugs in without further connector changes; both facets are asserted here.
 */
public class JdbcCredentialRefreshTests extends ESTestCase {

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

    public void testAuthFailedNotRetriedWithPerQueryCredentials() throws Exception {
        // Scoped/deferred behavior for the production per-query credential source: AUTH_FAILED (28000) fails fast on
        // the FIRST attempt, no fake refresh of immutable credentials.
        AtomicLong epoch = new AtomicLong(0);
        ScriptedAuthSource source = new ScriptedAuthSource(1, epoch, false);
        JdbcConnector connector = new JdbcConnector(source, GenericDialect.INSTANCE, jdbcUrl, nonRefreshableCredentials(), epoch::get);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> connector.execute(request(), (Split) null));
        assertEquals("per-query creds are not refreshable -> no retry", 1, source.calls.get());
        assertTrue("message carries category: " + e.getMessage(), e.getMessage().contains("category=[AUTH_FAILED]"));
        assertTrue(e.getMessage().contains("sqlstate=28000"));
    }

    public void testAuthFailedRetriedOnceWithRefreshableSourceAfterReload() throws Exception {
        // A refreshable (future keystore-backed) source: a reload bumps the epoch during the first (failing) borrow,
        // so the connector retries exactly once with the re-resolved (fresh) credential generation and succeeds.
        AtomicLong epoch = new AtomicLong(0);
        RecordingCredentials creds = new RecordingCredentials(epoch);
        ScriptedAuthSource source = new ScriptedAuthSource(1, epoch, true); // bump epoch on the failing borrow
        JdbcConnector connector = new JdbcConnector(source, GenericDialect.INSTANCE, jdbcUrl, creds, epoch::get);
        try (ResultCursor cursor = connector.execute(request(), (Split) null)) {
            assertNotNull(cursor);
        }
        assertEquals("exactly one retry (2 borrow attempts)", 2, source.calls.get());
        assertEquals("credentials re-resolved across two generations", List.of(0L, 1L), creds.generationsWritten);
    }

    public void testAuthFailedNotRetriedWhenNoReloadEvenIfRefreshable() throws Exception {
        // Refreshable source, but NO reload happened (epoch unchanged): retrying would just re-present the same
        // generation, so the connector must NOT retry -- fail fast on the first attempt.
        AtomicLong epoch = new AtomicLong(0);
        RecordingCredentials creds = new RecordingCredentials(epoch);
        ScriptedAuthSource source = new ScriptedAuthSource(1, epoch, false); // do NOT bump the epoch
        JdbcConnector connector = new JdbcConnector(source, GenericDialect.INSTANCE, jdbcUrl, creds, epoch::get);
        expectThrows(IllegalStateException.class, () -> connector.execute(request(), (Split) null));
        assertEquals("no reload -> no retry", 1, source.calls.get());
        assertEquals(List.of(0L), creds.generationsWritten);
    }

    public void testAuthRetryIsExactlyOnceThenPropagates() throws Exception {
        // Refreshable source, epoch bumped on every failing borrow, but the credential is bad on BOTH attempts: the
        // connector retries exactly once (never an unbounded loop) and then propagates.
        AtomicLong epoch = new AtomicLong(0);
        RecordingCredentials creds = new RecordingCredentials(epoch);
        ScriptedAuthSource source = new ScriptedAuthSource(2, epoch, true);
        JdbcConnector connector = new JdbcConnector(source, GenericDialect.INSTANCE, jdbcUrl, creds, epoch::get);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> connector.execute(request(), (Split) null));
        assertEquals("initial attempt + exactly one retry", 2, source.calls.get());
        assertTrue(e.getMessage().contains("category=[AUTH_FAILED]"));
    }

    public void testReloadBumpsEpochOnInstanceOwnedConfigNotStatic() throws Exception {
        // The ReloadablePlugin hook bumps the credential epoch on the plugin's INSTANCE-owned JdbcRuntimeConfig, and
        // two independent instances do not share it (no static bridge).
        try (JdbcDataSourcePlugin a = new JdbcDataSourcePlugin(); JdbcDataSourcePlugin b = new JdbcDataSourcePlugin()) {
            long a0 = a.runtimeConfig().credentialEpoch();
            long b0 = b.runtimeConfig().credentialEpoch();
            a.reload(Settings.EMPTY);
            assertEquals("reload must advance a's epoch", a0 + 1, a.runtimeConfig().credentialEpoch());
            assertEquals("b's epoch must be untouched (instance-owned, not static)", b0, b.runtimeConfig().credentialEpoch());
            a.reload(Settings.EMPTY);
            assertEquals("each reload is a distinct generation", a0 + 2, a.runtimeConfig().credentialEpoch());
        }
    }

    // -- helpers -------------------------------------------------------------------------------

    private static JdbcConnector.CredentialSource nonRefreshableCredentials() {
        return new JdbcConnector.CredentialSource() {
            @Override
            public void writeInto(Properties props) {}

            @Override
            public boolean refreshable() {
                return false; // mirrors production PerQueryCredentials
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
     * A refreshable {@link JdbcConnector.CredentialSource} that resolves the credential generation from the shared
     * epoch on each {@link #writeInto} and records which generations it produced, so a test can prove the retry
     * re-resolved fresh credentials.
     */
    private static final class RecordingCredentials implements JdbcConnector.CredentialSource {
        private final AtomicLong epoch;
        final List<Long> generationsWritten = new ArrayList<>();

        RecordingCredentials(AtomicLong epoch) {
            this.epoch = epoch;
        }

        @Override
        public void writeInto(Properties props) {
            long generation = epoch.get();
            generationsWritten.add(generation);
            props.setProperty("user", "user-gen-" + generation);
            props.setProperty("password", "pw-gen-" + generation);
        }

        @Override
        public boolean refreshable() {
            return true;
        }
    }

    /**
     * Fails its first {@code failCount} borrows with an {@code AUTH_FAILED} (SQLState {@code 28000}), optionally
     * bumping the shared epoch on each failure to model a reload delivering fresh credentials mid-flight, then hands
     * back a real in-process H2 connection.
     */
    private final class ScriptedAuthSource implements JdbcConnector.ConnectionSource {
        final AtomicInteger calls = new AtomicInteger();
        private final int failCount;
        private final AtomicLong epoch;
        private final boolean bumpEpochOnFailure;

        ScriptedAuthSource(int failCount, AtomicLong epoch, boolean bumpEpochOnFailure) {
            this.failCount = failCount;
            this.epoch = epoch;
            this.bumpEpochOnFailure = bumpEpochOnFailure;
        }

        @Override
        public Connection getConnection(String url, Properties props) throws SQLException {
            int n = calls.incrementAndGet();
            if (n <= failCount) {
                if (bumpEpochOnFailure) {
                    epoch.incrementAndGet();
                }
                throw new SQLException("access denied for user", "28000");
            }
            return DriverManager.getConnection(jdbcUrl);
        }
    }
}
