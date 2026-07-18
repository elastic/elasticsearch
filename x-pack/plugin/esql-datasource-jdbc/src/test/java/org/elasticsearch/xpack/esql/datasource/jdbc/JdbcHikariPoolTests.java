/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Unit tests for {@link JdbcHikariPool}: per-endpoint keying/normalization, HikariCP config wiring, the pool-timeout
 * to {@link IllegalStateException} translation, and instance-owned lifecycle (close tears down all pools; the pool is
 * a per-{@link JdbcDataSourcePlugin}-instance field, never a static).
 * <p>
 * Runs entirely against in-process H2 on the test classpath -- no Docker. Each test opens a keep-alive connection per
 * H2 in-mem URL so the database survives while HikariCP grows/shrinks its own pooled connections, and closes every
 * pool + keep-alive in {@code tearDown} so HikariCP's (daemon) housekeeping threads are stopped before thread-leak
 * detection runs.
 */
public class JdbcHikariPoolTests extends ESTestCase {

    private JdbcDriverRegistry registry;
    private final List<JdbcHikariPool> poolsToClose = new ArrayList<>();
    private final List<Connection> keepAlives = new ArrayList<>();

    /**
     * The two ownership tests construct a {@link JdbcDataSourcePlugin} and drive it through {@code connectors()}, which
     * only builds the pool when {@link JdbcDataSourcePlugin#ESQL_EXTERNAL_DATASOURCES_JDBC_FEATURE_FLAG} is on. That
     * flag is off in release builds ({@code -Dbuild.snapshot=false}), so those tests are skipped (not failed) there.
     * The direct-{@link JdbcHikariPool} tests below do not touch the plugin and run unconditionally.
     */
    private static void assumeJdbcFlagEnabled() {
        assumeTrue(
            "requires the esql_external_datasources_jdbc feature flag (off in release builds)",
            JdbcDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_JDBC_FEATURE_FLAG.isEnabled()
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = JdbcDriverRegistry.fromClassLoader(getClass().getClassLoader());
    }

    @Override
    public void tearDown() throws Exception {
        for (JdbcHikariPool pool : poolsToClose) {
            pool.close();
        }
        for (Connection c : keepAlives) {
            try {
                c.close();
            } catch (Exception e) {
                // best-effort test cleanup
            }
        }
        registry.close();
        super.tearDown();
    }

    private JdbcHikariPool newPool(JdbcRuntimeConfig config) {
        JdbcHikariPool pool = new JdbcHikariPool(registry, config);
        poolsToClose.add(pool);
        return pool;
    }

    /** Opens (and retains for the test's lifetime) a keep-alive connection so the H2 in-mem DB does not vanish. */
    private String h2Url() throws Exception {
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        keepAlives.add(registry.connect(url, new Properties()));
        return url;
    }

    private static JdbcRuntimeConfig config(int maxPerUrl, long connTimeoutMs, long idleMs, long maxLifeMs) {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.initialize(
            Settings.builder()
                .put(JdbcRuntimeConfig.POOL_MAX_PER_URL.getKey(), maxPerUrl)
                .put(JdbcRuntimeConfig.POOL_CONNECTION_TIMEOUT_MS.getKey(), connTimeoutMs)
                .put(JdbcRuntimeConfig.POOL_IDLE_TIMEOUT_MS.getKey(), idleMs)
                .put(JdbcRuntimeConfig.POOL_MAX_LIFETIME_MS.getKey(), maxLifeMs)
                .put(JdbcRuntimeConfig.ALLOW_LOOPBACK.getKey(), true)
                .build()
        );
        return config;
    }

    private static JdbcRuntimeConfig config(
        int maxPerUrl,
        long connTimeoutMs,
        long idleMs,
        long maxLifeMs,
        long keepaliveMs,
        long validationMs
    ) {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.initialize(
            Settings.builder()
                .put(JdbcRuntimeConfig.POOL_MAX_PER_URL.getKey(), maxPerUrl)
                .put(JdbcRuntimeConfig.POOL_CONNECTION_TIMEOUT_MS.getKey(), connTimeoutMs)
                .put(JdbcRuntimeConfig.POOL_IDLE_TIMEOUT_MS.getKey(), idleMs)
                .put(JdbcRuntimeConfig.POOL_MAX_LIFETIME_MS.getKey(), maxLifeMs)
                .put(JdbcRuntimeConfig.POOL_KEEPALIVE_MS.getKey(), keepaliveMs)
                .put(JdbcRuntimeConfig.POOL_VALIDATION_TIMEOUT_MS.getKey(), validationMs)
                .put(JdbcRuntimeConfig.ALLOW_LOOPBACK.getKey(), true)
                .build()
        );
        return config;
    }

    /** As {@link #config(int, long, long, long)} but also caps the pool cache at {@code maxPools}. */
    private static JdbcRuntimeConfig configWithMaxPools(int maxPools, int maxPerUrl, long connTimeoutMs) {
        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.initialize(
            Settings.builder()
                .put(JdbcRuntimeConfig.POOL_MAX_POOLS.getKey(), maxPools)
                .put(JdbcRuntimeConfig.POOL_MAX_PER_URL.getKey(), maxPerUrl)
                .put(JdbcRuntimeConfig.POOL_CONNECTION_TIMEOUT_MS.getKey(), connTimeoutMs)
                .put(JdbcRuntimeConfig.POOL_IDLE_TIMEOUT_MS.getKey(), 30000L)
                .put(JdbcRuntimeConfig.POOL_MAX_LIFETIME_MS.getKey(), 900000L)
                .put(JdbcRuntimeConfig.ALLOW_LOOPBACK.getKey(), true)
                .build()
        );
        return config;
    }

    // -- URL normalization / keying ------------------------------------------------------------

    public void testNormalizeKeyStripsUserinfo() {
        // Endpoint normalization strips URL-embedded userinfo (credentials do NOT travel in the URL; they arrive via
        // the per-query user/password properties). So the endpoint component is identical regardless of URL userinfo.
        assertEquals("jdbc:postgresql://host:5432/db", JdbcHikariPool.normalizeKey("jdbc:postgresql://alice:secret@host:5432/db"));
        assertEquals(
            JdbcHikariPool.normalizeKey("jdbc:postgresql://alice:secret@host:5432/db"),
            JdbcHikariPool.normalizeKey("jdbc:postgresql://bob:other@host:5432/db")
        );
    }

    public void testPoolKeyIncludesCredentialFingerprint() {
        // The full pool key = normalized endpoint + credential fingerprint. Same endpoint + DIFFERENT props credentials
        // must produce DIFFERENT keys (per-credential segregation); same endpoint + SAME credentials must match.
        String url = "jdbc:postgresql://host:5432/db";
        String keyAlice = JdbcHikariPool.poolKey(url, credentials("alice", "secret"));
        String keyBob = JdbcHikariPool.poolKey(url, credentials("bob", "other"));
        String keyAlice2 = JdbcHikariPool.poolKey(url, credentials("alice", "secret"));
        assertNotEquals("distinct credentials must yield distinct pool keys", keyAlice, keyBob);
        assertEquals("identical credentials must yield the same pool key", keyAlice, keyAlice2);
        // The key must carry the sanitized endpoint but never the raw credentials.
        assertTrue(keyAlice.startsWith("jdbc:postgresql://host:5432/db:"));
        assertFalse("pool key must not leak the raw password", keyAlice.contains("secret"));
        assertFalse("pool key must not leak the raw user", keyAlice.contains("alice"));
        // Null/absent user+password is deterministic and distinct from any real credential set.
        assertEquals(JdbcHikariPool.poolKey(url, new Properties()), JdbcHikariPool.poolKey(url, new Properties()));
        assertNotEquals(JdbcHikariPool.poolKey(url, new Properties()), keyAlice);
    }

    public void testPoolKeyDistinctForDifferentConnectionProperties() {
        // Same endpoint + SAME credentials but DIFFERENT connection_properties (e.g. a different
        // sslmode) configure physically different connections and MUST NOT share a pool.
        String url = "jdbc:postgresql://host:5432/db";
        Properties base = credentials("alice", "secret");
        Properties withRequire = credentials("alice", "secret");
        withRequire.setProperty("sslmode", "require");
        Properties withDisable = credentials("alice", "secret");
        withDisable.setProperty("sslmode", "disable");
        Properties withRequire2 = credentials("alice", "secret");
        withRequire2.setProperty("sslmode", "require");

        String keyBase = JdbcHikariPool.poolKey(url, base);
        String keyRequire = JdbcHikariPool.poolKey(url, withRequire);
        String keyDisable = JdbcHikariPool.poolKey(url, withDisable);

        assertNotEquals("no props vs sslmode=require must differ", keyBase, keyRequire);
        assertNotEquals("sslmode=require vs sslmode=disable must differ", keyRequire, keyDisable);
        assertEquals("identical connection_properties must match", keyRequire, JdbcHikariPool.poolKey(url, withRequire2));
        // The value is non-secret, but the key still carries only the sanitized endpoint + one-way hashes.
        assertTrue(keyRequire.startsWith("jdbc:postgresql://host:5432/db:"));
        assertFalse("pool key must not leak the raw credential", keyRequire.contains("secret"));

        // A tuning prop added on top of DIFFERENT credentials must still be distinct (independent dimensions).
        Properties bobRequire = credentials("bob", "other");
        bobRequire.setProperty("sslmode", "require");
        assertNotEquals(keyRequire, JdbcHikariPool.poolKey(url, bobRequire));
    }

    public void testPoolKeyStableAcrossAbsentEmptyWhitespaceConnectionProperties() {
        // Absent vs empty-string vs whitespace connection_properties must all normalize to the SAME
        // pool key, so they never spin up a needless second pool for the same endpoint+credentials. The connector
        // parses the raw string (JdbcConnectionProperties.parse -> empty map for null/blank) and layers it onto the
        // driver Properties before poolKey runs, so we reproduce that transform here and assert key stability.
        String url = "jdbc:postgresql://host:5432/db";
        Properties absent = credentials("alice", "secret");
        JdbcConnectionProperties.applyTo(absent, JdbcConnectionProperties.parse(null));
        Properties empty = credentials("alice", "secret");
        JdbcConnectionProperties.applyTo(empty, JdbcConnectionProperties.parse(""));
        Properties whitespace = credentials("alice", "secret");
        JdbcConnectionProperties.applyTo(whitespace, JdbcConnectionProperties.parse("   "));

        String keyAbsent = JdbcHikariPool.poolKey(url, absent);
        assertEquals("empty-string connection_properties must key identically to absent", keyAbsent, JdbcHikariPool.poolKey(url, empty));
        assertEquals("whitespace connection_properties must key identically to absent", keyAbsent, JdbcHikariPool.poolKey(url, whitespace));
        // And a real tuning prop must still produce a DIFFERENT key (guards against over-normalizing everything away).
        Properties withProp = credentials("alice", "secret");
        JdbcConnectionProperties.applyTo(withProp, JdbcConnectionProperties.parse("sslmode=require"));
        assertNotEquals(keyAbsent, JdbcHikariPool.poolKey(url, withProp));
    }

    public void testNormalizeKeyStripsQueryString() {
        assertEquals(
            "jdbc:postgresql://host:5432/db",
            JdbcHikariPool.normalizeKey("jdbc:postgresql://host:5432/db?user=alice&password=secret&ssl=true")
        );
    }

    public void testNormalizeKeyIsCaseInsensitive() {
        assertEquals(
            JdbcHikariPool.normalizeKey("jdbc:postgresql://HOST:5432/DB"),
            JdbcHikariPool.normalizeKey("jdbc:postgresql://host:5432/db")
        );
    }

    public void testNormalizeKeyLeavesAtInPathAlone() {
        // An '@' after the first path '/' is not userinfo and must not be stripped.
        assertEquals("jdbc:weird:foo/a@b", JdbcHikariPool.normalizeKey("jdbc:weird:foo/a@b"));
    }

    public void testDistinctEndpointsGetDistinctPools() throws Exception {
        JdbcHikariPool pool = newPool(config(2, 5000L, 30000L, 900000L));
        String urlA = h2Url();
        String urlB = h2Url();
        try (Connection a = pool.getConnection(urlA, new Properties()); Connection b = pool.getConnection(urlB, new Properties())) {
            assertNotNull(a);
            assertNotNull(b);
        }
        assertEquals(2, pool.poolCount());
        assertNotSame(pool.poolFor(urlA), pool.poolFor(urlB));
    }

    public void testSameEndpointSameCredentialsSharesOnePool() throws Exception {
        // Same endpoint borrowed twice with the SAME credentials reuses a single pool (physical connections shared).
        JdbcHikariPool pool = newPool(config(2, 5000L, 30000L, 900000L));
        String url = h2Url();
        try (Connection a = pool.getConnection(url, new Properties()); Connection b = pool.getConnection(url, new Properties())) {
            assertNotNull(a);
            assertNotNull(b);
        }
        assertEquals(1, pool.poolCount());
    }

    /**
     * Regression for the credential-isolation invariant: two DIFFERENT credential sets against ONE endpoint must
     * create TWO pools, and each borrowed connection must authenticate under its OWN credentials -- never the first
     * caller's (no "first-caller-wins" identity swap). Uses two real H2 users so {@code CURRENT_USER} proves which
     * identity each connection actually authenticated as.
     */
    public void testDifferentCredentialsSameEndpointGetDistinctPoolsAndOwnIdentity() throws Exception {
        JdbcHikariPool pool = newPool(config(2, 5000L, 30000L, 900000L));
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        Properties saCreds = credentials("sa", "sapass");
        // First connection to a fresh in-mem DB creates it and establishes SA's password; retain as keep-alive.
        Connection keepAlive = registry.connect(url, copyOf(saCreds));
        keepAlives.add(keepAlive);
        // Create a SECOND real H2 user (admin, so it can also run the identity probe).
        try (Statement st = keepAlive.createStatement()) {
            st.execute("CREATE USER BOB PASSWORD 'bobpass' ADMIN");
        }
        Properties bobCreds = credentials("bob", "bobpass");

        try (Connection asSa = pool.getConnection(url, copyOf(saCreds)); Connection asBob = pool.getConnection(url, copyOf(bobCreds))) {
            assertEquals("connection borrowed with SA creds must authenticate as SA", "SA", currentUser(asSa));
            // The core assertion: the second, differently-credentialed borrow runs as BOB, NOT as the first caller SA.
            assertEquals("connection borrowed with BOB creds must authenticate as BOB, not first-caller SA", "BOB", currentUser(asBob));
        }

        // Two distinct pools exist for the one endpoint (one per credential fingerprint), and they are not the same.
        assertEquals(2, pool.poolCount());
        assertNotNull(pool.poolFor(url, saCreds));
        assertNotNull(pool.poolFor(url, bobCreds));
        assertNotSame(pool.poolFor(url, saCreds), pool.poolFor(url, bobCreds));
    }

    private static String currentUser(Connection c) throws Exception {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT CURRENT_USER")) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static Properties credentials(String user, String password) {
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", password);
        return p;
    }

    // -- HikariCP config wiring ----------------------------------------------------------------

    public void testConfigValuesAppliedToHikariDataSource() throws Exception {
        JdbcHikariPool pool = newPool(config(7, 1234L, 15000L, 60000L));
        String url = h2Url();
        try (Connection c = pool.getConnection(url, new Properties())) {
            assertNotNull(c);
        }
        HikariDataSource ds = pool.poolFor(url);
        assertNotNull("a HikariDataSource must exist for the endpoint", ds);
        assertEquals(7, ds.getMaximumPoolSize());
        assertEquals(1234L, ds.getConnectionTimeout());
        assertEquals(15000L, ds.getIdleTimeout());
        assertEquals(60000L, ds.getMaxLifetime());
    }

    // -- keepalive + validationTimeout wiring + ordering invariant ------------------------

    public void testKeepaliveAndValidationAppliedToLiveHikariDataSource() throws Exception {
        // A valid, well-ordered configuration (30000 <= keepalive < idle < maxLifetime, validation <= connection)
        // must reach the live HikariDataSource unchanged. Uses a real H2 pool (no Docker).
        JdbcHikariPool pool = newPool(config(3, 5000L, 60000L, 900000L, 40000L, 3000L));
        String url = h2Url();
        try (Connection c = pool.getConnection(url, new Properties())) {
            assertNotNull(c);
        }
        HikariDataSource ds = pool.poolFor(url);
        assertNotNull(ds);
        assertEquals("keepaliveTime must reach HikariCP", 40000L, ds.getKeepaliveTime());
        assertEquals("validationTimeout must reach HikariCP", 3000L, ds.getValidationTimeout());
    }

    public void testValidationClampWarnDedupedPerUrl() throws Exception {
        // validation(9000) > connection(4000) triggers the clamp WARN. Two DIFFERENT credential sets
        // on the SAME endpoint create TWO pools, but the clamp/disable WARN is the endpoint's misconfiguration, so it
        // must fire ONCE per sanitized URL -- not once per (url × credential) pool.
        JdbcHikariPool pool = newPool(config(2, 4000L, 60000L, 900000L, 0L, 9000L));
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        Properties saCreds = credentials("sa", "sapass");
        Connection keepAlive = registry.connect(url, copyOf(saCreds));
        keepAlives.add(keepAlive);
        try (Statement st = keepAlive.createStatement()) {
            st.execute("CREATE USER BOB PASSWORD 'bobpass' ADMIN");
        }
        Properties bobCreds = credentials("bob", "bobpass");

        CountingExpectation clampWarn = new CountingExpectation(
            JdbcHikariPool.class.getName(),
            Level.WARN,
            "clamping validation_timeout_ms"
        );
        try (var mockLog = MockLog.capture(JdbcHikariPool.class)) {
            mockLog.addExpectation(clampWarn);
            try (Connection a = pool.getConnection(url, copyOf(saCreds)); Connection b = pool.getConnection(url, copyOf(bobCreds))) {
                assertNotNull(a);
                assertNotNull(b);
            }
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals("two credential-distinct pools were created for the one endpoint", 2, pool.poolCount());
        assertEquals("the validation clamp WARN must be deduped to once per sanitized URL", 1, clampWarn.count());
    }

    public void testApplyPoolSizingAndTimeoutsWiresAllKnobs() {
        // Build a bare HikariConfig via the package-private applier (no HikariDataSource, so no physical connection is
        // opened) and assert every knob — including the keepalive/validation — lands on the config.
        HikariConfig hc = new HikariConfig();
        JdbcHikariPool.applyPoolSizingAndTimeouts(hc, config(9, 5000L, 60000L, 900000L, 45000L, 2500L));
        assertEquals(9, hc.getMaximumPoolSize());
        assertEquals(0, hc.getMinimumIdle());
        assertEquals(5000L, hc.getConnectionTimeout());
        assertEquals(60000L, hc.getIdleTimeout());
        assertEquals(900000L, hc.getMaxLifetime());
        assertEquals(45000L, hc.getKeepaliveTime());
        assertEquals(2500L, hc.getValidationTimeout());
    }

    public void testValidationTimeoutClampedToConnectionTimeout() {
        // validationTimeout > connectionTimeout violates the invariant; it must be clamped DOWN to connectionTimeout.
        HikariConfig hc = new HikariConfig();
        JdbcHikariPool.applyPoolSizingAndTimeouts(hc, config(2, 4000L, 60000L, 900000L, 0L, 9000L));
        assertEquals("validationTimeout must be clamped to connectionTimeout", 4000L, hc.getValidationTimeout());
    }

    public void testKeepaliveDisabledWhenBelowHikariFloor() {
        // < 30000ms is below HikariCP's floor -> disabled (left at the HikariCP default 0), never handed to HikariCP.
        assertEquals(0L, JdbcHikariPool.effectiveKeepaliveMs(20_000L, 60_000L, 900_000L));
        HikariConfig hc = new HikariConfig();
        JdbcHikariPool.applyPoolSizingAndTimeouts(hc, config(2, 5000L, 60000L, 900000L, 20000L, 3000L));
        assertEquals("sub-floor keepalive must be disabled", 0L, hc.getKeepaliveTime());
    }

    public void testKeepaliveDisabledWhenNotBelowIdleTimeout() {
        // keepalive >= idle_timeout: an idle connection would be retired before the keepalive could fire -> disabled.
        assertEquals(0L, JdbcHikariPool.effectiveKeepaliveMs(40_000L, 30_000L, 900_000L));
        HikariConfig hc = new HikariConfig();
        JdbcHikariPool.applyPoolSizingAndTimeouts(hc, config(2, 5000L, 30000L, 900000L, 40000L, 3000L));
        assertEquals(0L, hc.getKeepaliveTime());
    }

    public void testKeepaliveDisabledWhenNotBelowMaxLifetime() {
        // keepalive >= max_lifetime -> disabled (this is exactly the relationship HikariCP would otherwise reset).
        assertEquals(0L, JdbcHikariPool.effectiveKeepaliveMs(40_000L, 100_000L, 35_000L));
    }

    public void testEffectiveKeepaliveValidPassesThrough() {
        // 30000 <= keepalive < idle < maxLifetime: the value passes through unchanged.
        assertEquals(40_000L, JdbcHikariPool.effectiveKeepaliveMs(40_000L, 60_000L, 900_000L));
        // Disabled (0) is always valid and stays disabled.
        assertEquals(0L, JdbcHikariPool.effectiveKeepaliveMs(0L, 60_000L, 900_000L));
    }

    // -- pool-timeout translation --------------------------------------------------------------

    public void testConnectionTimeoutTranslatedToIllegalState() throws Exception {
        // Pool of 1; hold the single connection, then a second borrow must fail fast (not block) with our sanitized
        // IllegalStateException carrying pool_max / in_use diagnostics.
        JdbcHikariPool pool = newPool(config(1, 300L, 30000L, 900000L));
        String url = h2Url();
        Connection held = pool.getConnection(url, new Properties());
        try {
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> pool.getConnection(url, new Properties()));
            assertTrue(
                "message must mention timeout budget: " + e.getMessage(),
                e.getMessage().contains("no JDBC connection available within 300ms")
            );
            assertTrue("message must carry the target: " + e.getMessage(), e.getMessage().contains("target="));
            assertTrue("message must carry pool_max: " + e.getMessage(), e.getMessage().contains("pool_max=1"));
            assertTrue("message must carry in_use: " + e.getMessage(), e.getMessage().contains("in_use=1"));
        } finally {
            held.close();
        }
    }

    public void testTimeoutMessageDoesNotLeakCredentials() throws Exception {
        // The endpoint pool is keyed on the sanitized URL; the translated message must not echo credentials even
        // though they were passed in the properties. Establish the H2 in-mem DB WITH these credentials (first
        // connection sets the sa password) so the pool can actually authenticate, then saturate + time out.
        JdbcHikariPool pool = newPool(config(1, 300L, 30000L, 900000L));
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        Properties creds = new Properties();
        creds.setProperty("user", "sa");
        creds.setProperty("password", "topsecret");
        // Keep-alive under the same credentials so the DB survives and the sa password is established.
        keepAlives.add(registry.connect(url, copyOf(creds)));
        Connection held = pool.getConnection(url, copyOf(creds));
        try {
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> pool.getConnection(url, copyOf(creds)));
            assertFalse("must not leak the password", e.getMessage().contains("topsecret"));
            assertTrue(e.getMessage().contains("no JDBC connection available within 300ms"));
        } finally {
            held.close();
        }
    }

    // -- AUTH-masking fix: unwrap a pool-acquisition timeout that hides an auth failure ------

    public void testAuthFailureCauseUnwrapsWrapped28000() {
        // A HikariCP-style acquisition timeout whose cause chain hides a driver auth failure (SQLState 28xxx) must be
        // recognized as an auth failure and the underlying SQLException returned (mirrors HikariCP, which also copies
        // the cause's SQLState onto the wrapper).
        SQLException auth = new SQLException("password authentication failed for user", "28000");
        SQLTransientConnectionException wrapper = new SQLTransientConnectionException(
            "pool - Connection is not available, request timed out after 250ms, (28000)",
            "28000",
            auth
        );
        assertSame(auth, JdbcHikariPool.authFailureCause(wrapper));
    }

    public void testAuthFailureCauseNullForPlainPoolExhaustion() {
        // A genuine pool-exhaustion timeout has no underlying driver exception -> not an auth failure -> null, so the
        // caller keeps the fast-fail pool-timeout translation.
        SQLTransientConnectionException wrapper = new SQLTransientConnectionException(
            "pool - Connection is not available, request timed out after 250ms",
            "08001",
            null
        );
        assertNull(JdbcHikariPool.authFailureCause(wrapper));
    }

    public void testAuthFailureCauseNullForTransientCause() {
        // A wrapped transient (connection-class 08xxx) failure is NOT an auth failure; it stays a pool timeout.
        SQLException transientFailure = new SQLException("connection refused", "08006");
        SQLTransientConnectionException wrapper = new SQLTransientConnectionException("timed out", "08006", transientFailure);
        assertNull(JdbcHikariPool.authFailureCause(wrapper));
    }

    public void testPoolAuthFailureSurfacesAsSqlExceptionNotPoolTimeout() throws Exception {
        // End-to-end through getConnection against real in-process H2: create the DB with a password, then borrow with
        // the WRONG password. HikariCP masks the driver's 28000 behind a SQLTransientConnectionException acquisition
        // timeout; the fix must UNWRAP it and surface a sanitized SQLException (SQLState 28xxx, classifiable as AUTH)
        // rather than the generic pool-timeout IllegalStateException -- and must not leak the wrong password.
        JdbcHikariPool pool = newPool(config(1, 500L, 30000L, 900000L));
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        Properties right = credentials("sa", "rightpass");
        // First connection establishes the sa password; retain as keep-alive so the in-mem DB survives.
        keepAlives.add(registry.connect(url, copyOf(right)));

        Properties wrong = credentials("sa", "wrongpass");
        // expectThrows(SQLException.class, ...) already proves the surfaced type is a checked SQLException and NOT the
        // pool-timeout IllegalStateException (a RuntimeException, which would fail this call).
        SQLException e = expectThrows(SQLException.class, () -> pool.getConnection(url, copyOf(wrong)));
        // The surfaced exception must be classifiable as an auth failure (28xxx), not swallowed as a pool timeout.
        assertEquals("AUTH must surface, not a generic pool timeout", JdbcSqlStateCategory.AUTH_FAILED, JdbcSqlStateClassifier.classify(e));
        assertFalse("must not leak the wrong password", messageChain(e).contains("wrongpass"));
    }

    private static String messageChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (t != null && depth++ < 32) {
            sb.append(t.toString()).append('\n');
            if (t.getMessage() != null) {
                sb.append(t.getMessage()).append('\n');
            }
            t = t.getCause();
        }
        return sb.toString();
    }

    private static Properties copyOf(Properties props) {
        Properties copy = new Properties();
        copy.putAll(props);
        return copy;
    }

    /**
     * A {@link MockLog.LoggingExpectation} that COUNTS every matching event (logger + level + message substring)
     * rather than just recording "seen at least once". Needed for the dedupe assertion, which must prove a WARN
     * fired exactly ONCE across two pool creations. {@link #assertMatched()} is a no-op (the test asserts the count
     * directly) but satisfies MockLog's "assertMatched called before release" contract.
     */
    private static final class CountingExpectation implements MockLog.LoggingExpectation {
        private final String logger;
        private final Level level;
        private final String substring;
        private final java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger();

        CountingExpectation(String logger, Level level, String substring) {
            this.logger = logger;
            this.level = level;
            this.substring = substring;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel().equals(level)
                && event.getLoggerName().equals(logger)
                && event.getMessage().getFormattedMessage().contains(substring)) {
                count.incrementAndGet();
            }
        }

        @Override
        public void assertMatched() {
            // no-op: the test asserts count() directly
        }

        int count() {
            return count.get();
        }
    }

    // -- bounded cache: LRU idle eviction on cap overflow -------------------------------------

    public void testPoolCacheEvictsIdleLruWhenOverCap() throws Exception {
        // Cap the cache at 2 pools. Creating a THIRD distinct-endpoint pool must evict and CLOSE the
        // least-recently-used IDLE pool, keeping the live count bounded at the cap.
        JdbcHikariPool pool = newPool(configWithMaxPools(2, 4, 5000L));
        String urlA = h2Url();
        String urlB = h2Url();
        String urlC = h2Url();
        pool.getConnection(urlA, new Properties()).close(); // A created + idle (oldest access)
        pool.getConnection(urlB, new Properties()).close(); // B created + idle
        HikariDataSource dsA = pool.poolFor(urlA);
        assertNotNull(dsA);
        assertEquals(2, pool.poolCount());
        // Touch B again so A is now the least-recently-used pool.
        pool.getConnection(urlB, new Properties()).close();
        // Creating C pushes the cache over the cap; the LRU idle pool (A) must be evicted + closed.
        pool.getConnection(urlC, new Properties()).close();
        assertEquals("cache must be bounded at the cap after eviction", 2, pool.poolCount());
        assertNull("LRU idle pool A must be evicted from the cache", pool.poolFor(urlA));
        assertTrue("evicted pool A must be closed", dsA.isClosed());
        assertNotNull("recently-used pool B must survive", pool.poolFor(urlB));
        assertNotNull("just-created pool C must survive", pool.poolFor(urlC));
    }

    public void testPoolCacheDoesNotEvictBusyPool() throws Exception {
        // Cap the cache at 1. Hold an ACTIVE connection on pool A, then create pool B. A cannot be evicted (it is in
        // use), so the cache temporarily exceeds the cap rather than closing an in-use pool.
        JdbcHikariPool pool = newPool(configWithMaxPools(1, 4, 5000L));
        String urlA = h2Url();
        String urlB = h2Url();
        Connection heldA = pool.getConnection(urlA, new Properties()); // A busy (active=1), intentionally NOT closed
        HikariDataSource dsA = pool.poolFor(urlA);
        assertNotNull(dsA);
        try {
            try (Connection connB = pool.getConnection(urlB, new Properties())) {
                assertNotNull(connB);
            }
            assertNotNull("busy pool A must NOT be evicted", pool.poolFor(urlA));
            assertFalse("busy pool A must remain open", dsA.isClosed());
            assertEquals("cache is allowed to temporarily exceed the cap when all candidates are busy", 2, pool.poolCount());
        } finally {
            heldA.close();
        }
    }

    public void testPoolCacheCapHonoredAcrossManyEndpoints() throws Exception {
        // Create many distinct-endpoint pools with a small cap; the live count must never exceed the cap because each
        // borrowed connection is closed immediately (every prior pool is idle and evictable).
        int cap = 3;
        JdbcHikariPool pool = newPool(configWithMaxPools(cap, 4, 5000L));
        for (int i = 0; i < 10; i++) {
            pool.getConnection(h2Url(), new Properties()).close();
            assertTrue("pool count must stay within the cap, was " + pool.poolCount(), pool.poolCount() <= cap);
        }
        assertEquals(cap, pool.poolCount());
    }

    // -- lifecycle: instance-owned, close tears down ------------------------------------------

    public void testCloseClosesAllPoolsAndRejectsFurtherUse() throws Exception {
        JdbcHikariPool pool = new JdbcHikariPool(registry, config(2, 5000L, 30000L, 900000L));
        String urlA = h2Url();
        String urlB = h2Url();
        pool.getConnection(urlA, new Properties()).close();
        pool.getConnection(urlB, new Properties()).close();
        HikariDataSource dsA = pool.poolFor(urlA);
        HikariDataSource dsB = pool.poolFor(urlB);
        assertNotNull(dsA);
        assertNotNull(dsB);

        pool.close();

        assertTrue(pool.isClosed());
        assertTrue("underlying HikariDataSource A must be closed", dsA.isClosed());
        assertTrue("underlying HikariDataSource B must be closed", dsB.isClosed());
        assertEquals(0, pool.poolCount());
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> pool.getConnection(urlA, new Properties()));
        assertTrue(e.getMessage().contains("closed"));
    }

    // -- ownership: the plugin owns the pool as an INSTANCE field, released on close(), not a static ------

    public void testPluginOwnsPoolAsInstanceFieldReleasedOnClose() throws Exception {
        assumeJdbcFlagEnabled();
        try (JdbcDataSourcePlugin a = new JdbcDataSourcePlugin(); JdbcDataSourcePlugin b = new JdbcDataSourcePlugin()) {
            assertNotNull(a.connectors(Settings.EMPTY).get("jdbc"));
            assertNotNull(b.connectors(Settings.EMPTY).get("jdbc"));

            JdbcHikariPool poolA = a.hikariPoolOrNull();
            JdbcHikariPool poolB = b.hikariPoolOrNull();
            assertNotNull("connectors() must build the instance-owned pool", poolA);
            assertNotNull(poolB);
            assertNotSame("each plugin instance owns its own pool (no static)", poolA, poolB);

            // Closing a releases only a's pool; b's stays live.
            a.close();
            assertNull("a's pool reference released on close", a.hikariPoolOrNull());
            assertTrue("a's pool is closed", poolA.isClosed());
            assertNotNull("b's pool untouched by a.close()", b.hikariPoolOrNull());
            assertFalse("b's pool still open", poolB.isClosed());
        }
    }

    public void testPluginCloseTearsDownUnderlyingHikariDataSource() throws Exception {
        assumeJdbcFlagEnabled();
        JdbcDataSourcePlugin plugin = new JdbcDataSourcePlugin();
        try {
            assertNotNull(plugin.connectors(Settings.EMPTY).get("jdbc"));
            JdbcHikariPool pool = plugin.hikariPoolOrNull();
            assertNotNull(pool);
            String url = h2Url();
            pool.getConnection(url, new Properties()).close();
            HikariDataSource ds = pool.poolFor(url);
            assertNotNull(ds);

            plugin.close();

            assertTrue("plugin.close() must tear down the pool", pool.isClosed());
            assertTrue("plugin.close() must close the underlying HikariDataSource", ds.isClosed());
        } finally {
            plugin.close();
        }
    }
}
