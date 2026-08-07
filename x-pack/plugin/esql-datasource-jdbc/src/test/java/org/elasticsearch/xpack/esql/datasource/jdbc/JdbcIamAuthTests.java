/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Connector-side wiring tests for Redshift IAM / generic token authentication. These are the
 * LOCAL, no-AWS half of the auth story: the IAM credential EXCHANGE (GetClusterCredentials/STS) is the driver's job
 * and is deferred. What we own and verify here, entirely against in-process H2 via the
 * {@link JdbcConnector.ConnectionSource} seam and the production {@link JdbcConnector.PerQueryCredentials}:
 * <ul>
 *   <li>Typed AWS credentials supplied as {@link SecureString}s reach the driver {@link Properties} under the driver's
 *       {@code AccessKeyID}/{@code SecretAccessKey}/{@code SessionToken} names.</li>
 *   <li>Those secrets (and a token supplied via {@code password}) never appear in the wrapped exception chain OR in
 *       any log line even when the driver echoes them verbatim (planted-sentinel style): the failure path is wrapped
 *       in {@link MockLog} captures over every connection/error-path logger, at TRACE, with an
 *       {@link MockLog.UnseenEventExpectation} per sentinel.</li>
 *   <li>Ambient-chain mode: no explicit AWS creds => nothing AWS-related is written (driver uses its default chain).</li>
 *   <li>Generic token-as-password reaches the driver as {@code password} (Azure AD / Neon).</li>
 * </ul>
 */
public class JdbcIamAuthTests extends ESTestCase {

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

    public void testTypedAwsCredentialsReachDriverPropertiesFromSecureStrings() throws Exception {
        // Explicit-creds mode: the three AWS secrets, supplied as SecureStrings, must arrive at the driver under the
        // documented AccessKeyID/SecretAccessKey/SessionToken property names. Uses the PRODUCTION PerQueryCredentials
        // so this exercises the real credential-writing path, not a test stand-in.
        AtomicReference<Properties> captured = new AtomicReference<>();
        JdbcConnector.ConnectionSource capturingSource = (url, props) -> {
            captured.set((Properties) props.clone());
            return DriverManager.getConnection(jdbcUrl);
        };
        JdbcConnector.PerQueryCredentials creds = new JdbcConnector.PerQueryCredentials(
            secure("iam-role-user"),
            secure("token-shaped-password"),
            secure("AKIAEXAMPLEKEYID"),
            secure("wJalrXUtnFEMI-example-secret"),
            secure("FQoGZXIvYXdz-example-session-token")
        );
        // The dialect is irrelevant to credential writing; use GenericDialect so no vendor init SQL runs against H2.
        JdbcConnector connector = new JdbcConnector(capturingSource, GenericDialect.INSTANCE, jdbcUrl, creds, Map.of(), () -> 0L);
        try (ResultCursor cursor = connector.execute(request(), (Split) null)) {
            assertNotNull(cursor);
        }
        Properties props = captured.get();
        assertNotNull("connection source must have been invoked", props);
        assertEquals("AKIAEXAMPLEKEYID", props.getProperty("AccessKeyID"));
        assertEquals("wJalrXUtnFEMI-example-secret", props.getProperty("SecretAccessKey"));
        assertEquals("FQoGZXIvYXdz-example-session-token", props.getProperty("SessionToken"));
        // user/password ride the same channel and are still written.
        assertEquals("iam-role-user", props.getProperty("user"));
        assertEquals("token-shaped-password", props.getProperty("password"));
    }

    public void testAmbientChainModeWritesNoAwsCredentials() throws Exception {
        // No explicit AWS creds => the connector writes nothing AWS-related; the driver falls back to the ambient AWS
        // credential chain (env / instance-profile). Only the (optional) user is present here.
        AtomicReference<Properties> captured = new AtomicReference<>();
        JdbcConnector.ConnectionSource capturingSource = (url, props) -> {
            captured.set((Properties) props.clone());
            return DriverManager.getConnection(jdbcUrl);
        };
        JdbcConnector.PerQueryCredentials creds = new JdbcConnector.PerQueryCredentials(secure("iam-role-user"), null);
        JdbcConnector connector = new JdbcConnector(capturingSource, GenericDialect.INSTANCE, jdbcUrl, creds, Map.of(), () -> 0L);
        try (ResultCursor cursor = connector.execute(request(), (Split) null)) {
            assertNotNull(cursor);
        }
        Properties props = captured.get();
        assertNotNull(props);
        assertNull("no AccessKeyID in ambient-chain mode", props.getProperty("AccessKeyID"));
        assertNull("no SecretAccessKey in ambient-chain mode", props.getProperty("SecretAccessKey"));
        assertNull("no SessionToken in ambient-chain mode", props.getProperty("SessionToken"));
        assertEquals("iam-role-user", props.getProperty("user"));
    }

    public void testGenericTokenAsPasswordReachesDriver() throws Exception {
        // Azure AD / Neon token auth: a bearer token supplied via the existing password channel reaches the driver
        // as the "password" property unchanged. No AWS involvement.
        String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.header.signature";
        AtomicReference<Properties> captured = new AtomicReference<>();
        JdbcConnector.ConnectionSource capturingSource = (url, props) -> {
            captured.set((Properties) props.clone());
            return DriverManager.getConnection(jdbcUrl);
        };
        JdbcConnector.PerQueryCredentials creds = new JdbcConnector.PerQueryCredentials(secure("aad-user@example.com"), secure(token));
        JdbcConnector connector = new JdbcConnector(capturingSource, GenericDialect.INSTANCE, jdbcUrl, creds, Map.of(), () -> 0L);
        try (ResultCursor cursor = connector.execute(request(), (Split) null)) {
            assertNotNull(cursor);
        }
        assertEquals("bearer token must reach the driver as the password property", token, captured.get().getProperty("password"));
    }

    public void testPlantedSecretsNeverLeakIntoExceptionOrLogs() throws Exception {
        // Plant DISTINCT sentinels in each secret, then have the (mock) driver ECHO them all in an auth-failure
        // exception message -- the worst case, since some real drivers embed connection properties in their errors.
        // The connector must classify AUTH_FAILED, fail fast, and surface a SANITIZED exception; no sentinel may
        // appear anywhere in the thrown exception chain OR in any log line. The sentinels are alpha-only so they are
        // valid Regex simple-match literals in the UnseenEventExpectation patterns below.
        String pwdSentinel = "PWDSENTINEL" + randomAlphaOfLength(12);
        String akSentinel = "AKSENTINEL" + randomAlphaOfLength(12);
        String skSentinel = "SKSENTINEL" + randomAlphaOfLength(12);
        String stSentinel = "STSENTINEL" + randomAlphaOfLength(12);
        List<String> sentinels = List.of(pwdSentinel, akSentinel, skSentinel, stSentinel);

        JdbcConnector.ConnectionSource echoingDriver = (url, props) -> {
            // A driver that (badly) echoes the credential properties verbatim in its SQLException message.
            throw new SQLException(
                "FATAL: authentication failed; props were password="
                    + pwdSentinel
                    + " AccessKeyID="
                    + akSentinel
                    + " SecretAccessKey="
                    + skSentinel
                    + " SessionToken="
                    + stSentinel,
                "28000"
            );
        };
        JdbcConnector.PerQueryCredentials creds = new JdbcConnector.PerQueryCredentials(
            secure("iam-role-user"),
            secure(pwdSentinel),
            secure(akSentinel),
            secure(skSentinel),
            secure(stSentinel)
        );
        JdbcConnector connector = new JdbcConnector(echoingDriver, RedshiftDialect.INSTANCE, jdbcUrl, creds, Map.of(), () -> 0L);

        // Every logger that could plausibly touch the connection / error path. Force each to TRACE for the duration
        // of this test so that even a DEBUG/TRACE statement that (mis)handled a secret would actually be dispatched to
        // the MockLog appender and trip an UnseenEventExpectation -- proving the no-leak invariant even against the
        // most verbose logging config, not just the default level.
        Class<?>[] loggerClasses = {
            JdbcConnector.class,
            JdbcConnectorFactory.class,
            JdbcHikariPool.class,
            JdbcResultCursor.class,
            JdbcDriverRegistry.class };
        Level[] restore = new Level[loggerClasses.length];
        for (int i = 0; i < loggerClasses.length; i++) {
            var l4j = org.apache.logging.log4j.LogManager.getLogger(loggerClasses[i]);
            restore[i] = l4j.getLevel();
            Loggers.setLevel(l4j, Level.TRACE);
        }

        IllegalStateException e;
        try (MockLog mockLog = MockLog.capture(loggerClasses)) {
            // An UnseenEventExpectation per (logger, level, sentinel): the test FAILS if any sentinel ever appears in a
            // log MESSAGE on any of these loggers at any level. Levels are enumerated because MockLog matches an event
            // only against an expectation of the SAME level.
            for (Class<?> loggerClass : loggerClasses) {
                for (Level level : List.of(Level.FATAL, Level.ERROR, Level.WARN, Level.INFO, Level.DEBUG, Level.TRACE)) {
                    for (String sentinel : sentinels) {
                        mockLog.addExpectation(
                            new MockLog.UnseenEventExpectation(
                                "secret [" + sentinel + "] leaked into a [" + level + "] log on " + loggerClass.getSimpleName(),
                                loggerClass.getCanonicalName(),
                                level,
                                "*" + sentinel + "*"
                            )
                        );
                    }
                }
            }
            // UnseenEventExpectation only inspects LogEvent#getMessage() (the formatted message), NOT the logged
            // THROWABLE. A regression like `logger.warn("connection failed", rawSqlException)` would render a sentinel
            // in real logs via the throwable's message/stack trace while the message-pattern expectations stay silent.
            // Close that gap with an expectation that scans BOTH the formatted message AND the logged throwable chain
            // (cause + suppressed + SQLException#getNextException + full stack trace) of every captured event.
            NoSentinelInLoggedThrowableExpectation throwableScan = new NoSentinelInLoggedThrowableExpectation(sentinels);
            mockLog.addExpectation(throwableScan);
            e = expectThrows(IllegalStateException.class, () -> connector.execute(request(), (Split) null));
            // Fails if any sentinel reached a log message OR a logged throwable's chain.
            mockLog.assertAllExpectationsMatched();
        } finally {
            for (int i = 0; i < loggerClasses.length; i++) {
                Loggers.setLevel(org.apache.logging.log4j.LogManager.getLogger(loggerClasses[i]), restore[i]);
            }
        }

        // It must be classified as an auth failure (surfaced, not swallowed), and be fully sanitized.
        assertTrue("must surface AUTH_FAILED: " + e.getMessage(), e.getMessage().contains("category=[AUTH_FAILED]"));
        for (String sentinel : sentinels) {
            for (String text : allMessages(e)) {
                assertFalse("secret leaked into exception chain: " + text, text.contains(sentinel));
            }
        }
    }

    // -- helpers -------------------------------------------------------------------------------

    private static SecureString secure(String value) {
        return new SecureString(value.toCharArray());
    }

    /** Collects the message of a throwable and every cause/suppressed message so a sentinel scan is exhaustive. */
    private static List<String> allMessages(Throwable t) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        while (t != null && depth++ < 32) {
            if (t.getMessage() != null) {
                out.add(t.getMessage());
            }
            out.add(t.toString());
            for (Throwable s : t.getSuppressed()) {
                if (s.getMessage() != null) {
                    out.add(s.getMessage());
                }
            }
            if (t instanceof SQLException sql && sql.getNextException() != null && sql.getNextException() != t) {
                if (sql.getNextException().getMessage() != null) {
                    out.add(sql.getNextException().getMessage());
                }
            }
            t = t.getCause();
        }
        return out;
    }

    /**
     * Renders a throwable AND everything reachable from it to a single string the way a log appender would, so a
     * sentinel scan sees whatever a real log line would print. {@link Throwable#printStackTrace(PrintWriter)} already
     * recurses into the cause chain and suppressed throwables; on top of that we walk the {@link SQLException}
     * {@code getNextException()} chain (which is NOT a cause and so is not printed by the stack trace) and render each
     * of those too.
     */
    private static String renderThrowable(Throwable thrown) {
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            thrown.printStackTrace(pw);
        }
        StringBuilder out = new StringBuilder(sw.toString());
        // Belt-and-braces: getNextException is not part of the cause chain, so printStackTrace misses it.
        int depth = 0;
        Throwable t = thrown;
        while (t != null && depth++ < 32) {
            if (t instanceof SQLException sql) {
                SQLException next = sql.getNextException();
                if (next != null && next != sql) {
                    StringWriter nextSw = new StringWriter();
                    try (PrintWriter pw = new PrintWriter(nextSw)) {
                        next.printStackTrace(pw);
                    }
                    out.append('\n').append(nextSw);
                }
            }
            t = t.getCause();
        }
        return out.toString();
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
     * A {@link MockLog.LoggingExpectation} that captures EVERY {@link LogEvent} on the subscribed loggers and, on
     * {@link #assertMatched()}, fails if any planted sentinel appears in either the formatted MESSAGE or the logged
     * THROWABLE (rendered across its cause chain, suppressed throwables, {@link SQLException#getNextException()}, and
     * full stack trace). This closes the gap left by {@link MockLog.UnseenEventExpectation}, which matches only
     * {@link LogEvent#getMessage()} and would therefore miss a secret carried in a logged throwable such as
     * {@code logger.warn("failed", rawSqlException)}.
     */
    private static final class NoSentinelInLoggedThrowableExpectation implements MockLog.LoggingExpectation {
        private final List<String> sentinels;
        // match() is invoked from the logging call site (the test thread here), but use a concurrent list defensively.
        private final List<String> leaks = new CopyOnWriteArrayList<>();

        NoSentinelInLoggedThrowableExpectation(List<String> sentinels) {
            this.sentinels = sentinels;
        }

        @Override
        public void match(LogEvent event) {
            StringBuilder rendered = new StringBuilder(event.getMessage().getFormattedMessage());
            Throwable thrown = event.getThrown();
            if (thrown != null) {
                rendered.append('\n').append(renderThrowable(thrown));
            }
            String text = rendered.toString();
            for (String sentinel : sentinels) {
                if (text.contains(sentinel)) {
                    leaks.add(
                        "sentinel ["
                            + sentinel
                            + "] leaked into a ["
                            + event.getLevel()
                            + "] log event on ["
                            + event.getLoggerName()
                            + "] (message or logged throwable)"
                    );
                }
            }
        }

        @Override
        public void assertMatched() {
            if (leaks.isEmpty() == false) {
                throw new AssertionError("secret leaked into a logged message or throwable chain: " + leaks);
            }
        }
    }
}
