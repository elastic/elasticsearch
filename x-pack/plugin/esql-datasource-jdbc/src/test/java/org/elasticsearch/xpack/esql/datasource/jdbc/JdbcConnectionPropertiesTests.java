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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the allowlisted {@code connection_properties} passthrough:
 * {@link JdbcConnectionProperties} parse/allowlist/blocklist/secret enforcement + applyTo precedence, plus an
 * end-to-end check (via the {@link JdbcConnector.ConnectionSource} seam over in-process H2, no Docker) that an
 * allowlisted property actually reaches the driver {@link Properties} while the typed credentials still win.
 */
public class JdbcConnectionPropertiesTests extends ESTestCase {

    // -- parse: allowlist + canonicalization -------------------------------------------------

    public void testParseCanonicalizesAllowlistedKeysCaseInsensitively() {
        Map<String, String> parsed = JdbcConnectionProperties.parse("applicationname=es;SSLMODE=require; tcpKeepAlive =true");
        assertEquals("es", parsed.get("ApplicationName"));
        assertEquals("require", parsed.get("sslmode"));
        assertEquals("true", parsed.get("tcpKeepAlive"));
        assertEquals(3, parsed.size());
    }

    public void testParseKeepsEqualsInValue() {
        // pgjdbc / Neon SNI routing: options=endpoint=ep-x. Split on the FIRST '=' only.
        Map<String, String> parsed = JdbcConnectionProperties.parse("options=endpoint=ep-x");
        assertEquals("endpoint=ep-x", parsed.get("options"));
    }

    public void testParseNullAndBlankYieldEmptyMap() {
        assertTrue(JdbcConnectionProperties.parse(null).isEmpty());
        assertTrue(JdbcConnectionProperties.parse("   ").isEmpty());
        // A stray trailing ';' is a no-op, not an error.
        assertEquals(Map.of("sslmode", "require"), JdbcConnectionProperties.parse("sslmode=require;"));
    }

    // -- parse: default-deny + explicit footgun block ----------------------------------------

    public void testParseRejectsBlockedFootgun() {
        // socketFactory can load an arbitrary class -> RCE + connection re-point. Must be rejected with a clear error
        // that names the key but NOT the value.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> JdbcConnectionProperties.parse("socketFactory=com.evil.Factory")
        );
        assertTrue("message must name the key: " + e.getMessage(), e.getMessage().contains("socketFactory"));
        assertTrue("message must say blocked: " + e.getMessage(), e.getMessage().contains("blocked"));
        assertFalse("message must not echo the value", e.getMessage().contains("com.evil.Factory"));
    }

    public void testParseRejectsFileAccessFootguns() {
        for (String key : List.of("loggerFile", "sslcert", "sslkey", "sslrootcert", "authenticationPluginClassName")) {
            expectThrows(IllegalArgumentException.class, () -> JdbcConnectionProperties.parse(key + "=/etc/passwd"));
        }
    }

    public void testParseRejectsKeyNotOnAllowlist() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> JdbcConnectionProperties.parse("someRandomKnob=1"));
        assertTrue(e.getMessage().contains("someRandomKnob"));
        assertTrue("must list the allowlist: " + e.getMessage(), e.getMessage().contains("not permitted"));
    }

    // -- parse: secrets must use the typed channel -------------------------------------------

    public void testParseRejectsCredentialKeys() {
        for (String secret : List.of("user", "username", "password", "pwd", "passwd", "auth")) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> JdbcConnectionProperties.parse(secret + "=hunter2")
            );
            assertTrue("message must name the credential key: " + e.getMessage(), e.getMessage().contains(secret));
            assertTrue("message must point at the typed channel: " + e.getMessage(), e.getMessage().contains("user/password"));
            assertFalse("message must not echo the secret value", e.getMessage().contains("hunter2"));
        }
    }

    // -- Redshift IAM non-secret params -----------------------------------

    public void testParseAllowsRedshiftIamNonSecretParams() {
        // The non-secret IAM knobs pass the allowlist and canonicalize to the driver's property casing.
        Map<String, String> parsed = JdbcConnectionProperties.parse(
            "IAM=1;DbUser=analyst;ClusterID=my-cluster;Region=us-east-1;AutoCreate=true;DbGroups=readers"
        );
        assertEquals("1", parsed.get("IAM"));
        assertEquals("analyst", parsed.get("DbUser"));
        assertEquals("my-cluster", parsed.get("ClusterID"));
        assertEquals("us-east-1", parsed.get("Region"));
        assertEquals("true", parsed.get("AutoCreate"));
        assertEquals("readers", parsed.get("DbGroups"));
        assertEquals(6, parsed.size());
    }

    public void testParseIamParamsCaseInsensitive() {
        Map<String, String> parsed = JdbcConnectionProperties.parse("dbuser=analyst;region=eu-west-1;iam=1");
        assertEquals("analyst", parsed.get("DbUser"));
        assertEquals("eu-west-1", parsed.get("Region"));
        assertEquals("1", parsed.get("IAM"));
    }

    public void testParseRejectsAwsSecretKeysFromNonSecretMap() {
        // The SECRET AWS credentials must NOT ride the non-secret connection_properties map -- they belong to the
        // typed SecureString channel. Both the config-key form and the driver-property form are rejected as
        // credentials, and the secret value is never echoed.
        for (String secret : List.of(
            "access_key_id",
            "secret_access_key",
            "session_token",
            "AccessKeyID",
            "SecretAccessKey",
            "SessionToken"
        )) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> JdbcConnectionProperties.parse(secret + "=AKIAWILLNOTLEAK")
            );
            assertTrue("message must name the key: " + e.getMessage(), e.getMessage().contains(secret));
            assertTrue("message must point at the typed channel: " + e.getMessage(), e.getMessage().contains("user/password"));
            assertFalse("message must not echo the secret value", e.getMessage().contains("AKIAWILLNOTLEAK"));
        }
    }

    public void testParseRejectsRedshiftPluginNameFootgun() {
        // Plugin_Name loads an arbitrary auth-plugin class (RCE). It is in the EXPLICIT BLOCKED set (not merely
        // default-denied), so it is rejected with the pointed "blocked" footgun message and names only the key.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> JdbcConnectionProperties.parse("Plugin_Name=com.evil.Plugin")
        );
        assertTrue("message must name the key: " + e.getMessage(), e.getMessage().contains("Plugin_Name"));
        assertTrue("message must say blocked: " + e.getMessage(), e.getMessage().contains("blocked"));
        assertFalse("must not echo the value", e.getMessage().contains("com.evil.Plugin"));
        assertTrue("plugin_name must be in the explicit BLOCKED set", JdbcConnectionProperties.BLOCKED.contains("plugin_name"));
    }

    public void testParseRejectsMalformedEntry() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> JdbcConnectionProperties.parse("noEqualsHere"));
        assertTrue(e.getMessage().contains("expected"));
    }

    public void testParseValueCannotContainSemicolonDelimiter() {
        // ';' is the pair delimiter, so it cannot appear INSIDE a value -- the text after ';' is
        // parsed as the next pair. A value that embeds ';' must therefore FAIL CLEANLY (IllegalArgumentException),
        // not silently truncate. Here the trailing "extra" fragment has no '=' -> the "malformed entry" error, and
        // the (possibly sensitive) fragment is never echoed.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> JdbcConnectionProperties.parse("options=endpoint=ep;extra")
        );
        assertTrue("must be the malformed-entry error: " + e.getMessage(), e.getMessage().contains("expected"));
        assertFalse("must not echo the trailing value fragment", e.getMessage().contains("extra"));
    }

    // -- assertUrlHasNoBlockedProperties: BLOCKED footguns riding the JDBC URL --------------

    public void testUrlBlockedPropertyQueryParamRejected() {
        // A BLOCKED footgun can ride the URL query string straight into Driver.connect, bypassing the
        // connection_properties allowlist. It must be rejected, naming the KEY but never the value.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> JdbcConnectionProperties.assertUrlHasNoBlockedProperties("jdbc:postgresql://h/db?socketFactory=com.evil.Factory")
        );
        assertTrue("message must name the key: " + e.getMessage(), e.getMessage().contains("socketFactory"));
        assertTrue("message must say blocked: " + e.getMessage(), e.getMessage().contains("blocked"));
        assertFalse("message must not echo the value", e.getMessage().contains("com.evil.Factory"));
    }

    public void testUrlBlockedPropertySemicolonRejected() {
        // SQL Server / Sybase property-list form: ;key=value. Plugin_Name loads an arbitrary class (RCE).
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> JdbcConnectionProperties.assertUrlHasNoBlockedProperties("jdbc:sqlserver://h;Plugin_Name=com.evil.Plugin")
        );
        assertTrue("message must name the key: " + e.getMessage(), e.getMessage().contains("Plugin_Name"));
        assertTrue("message must say blocked: " + e.getMessage(), e.getMessage().contains("blocked"));
        assertFalse("message must not echo the value", e.getMessage().contains("com.evil.Plugin"));
    }

    public void testUrlBlockedPropertyAfterAmpersandRejected() {
        // A blocked key as a non-first query parameter (preceded by '&') is still caught.
        expectThrows(
            IllegalArgumentException.class,
            () -> JdbcConnectionProperties.assertUrlHasNoBlockedProperties(
                "jdbc:postgresql://h/db?ApplicationName=es&sslfactory=com.evil.Ssl"
            )
        );
    }

    public void testUrlBlockedPropertyCaseInsensitive() {
        expectThrows(
            IllegalArgumentException.class,
            () -> JdbcConnectionProperties.assertUrlHasNoBlockedProperties("jdbc:postgresql://h/db?SOCKETFACTORY=x")
        );
    }

    public void testUrlWithoutBlockedPropertyAccepted() {
        // A clean URL and an allowlisted tuning property (sslmode=require) must NOT be rejected.
        JdbcConnectionProperties.assertUrlHasNoBlockedProperties("jdbc:postgresql://h/db");
        JdbcConnectionProperties.assertUrlHasNoBlockedProperties("jdbc:postgresql://h/db?sslmode=require");
        JdbcConnectionProperties.assertUrlHasNoBlockedProperties(null);
        JdbcConnectionProperties.assertUrlHasNoBlockedProperties("");
    }

    public void testUrlBlockedTokenAsSuffixOfHarmlessKeyNotRejected() {
        // A blocked token that is merely a SUFFIX of a longer, harmless property name (preceded by a letter, not a
        // property delimiter) must not be flagged.
        JdbcConnectionProperties.assertUrlHasNoBlockedProperties("jdbc:postgresql://h/db?mysocketFactory=fine");
    }

    // -- TLS-disable policy (esql.jdbc.allow_plaintext) --------------------------------------

    public void testParseRejectsSslModeDisableByDefault() {
        // sslmode=disable puts credentials on the wire in cleartext; rejected unless allow_plaintext is set.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> JdbcConnectionProperties.parse("sslmode=disable"));
        assertTrue("message must name the key: " + e.getMessage(), e.getMessage().contains("sslmode"));
        assertTrue("message must point at the opt-in: " + e.getMessage(), e.getMessage().contains("allow_plaintext"));
    }

    public void testParseRejectsSslModeDisableCaseInsensitive() {
        expectThrows(IllegalArgumentException.class, () -> JdbcConnectionProperties.parse("SSLMODE=DISABLE"));
    }

    public void testParseAllowsSslModeDisableWhenAllowPlaintext() {
        Map<String, String> parsed = JdbcConnectionProperties.parse("sslmode=disable", true);
        assertEquals("disable", parsed.get("sslmode"));
    }

    public void testParseRejectsSslFalseByDefault() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> JdbcConnectionProperties.parse("ssl=false"));
        assertTrue("message must name the key: " + e.getMessage(), e.getMessage().contains("ssl"));
        assertTrue("message must point at the opt-in: " + e.getMessage(), e.getMessage().contains("allow_plaintext"));
    }

    public void testParseAllowsSslFalseWhenAllowPlaintext() {
        Map<String, String> parsed = JdbcConnectionProperties.parse("ssl=false", true);
        assertEquals("false", parsed.get("ssl"));
    }

    public void testParseAllowsSslModeRequireAlways() {
        // The safe, common case must always be accepted regardless of allow_plaintext.
        assertEquals("require", JdbcConnectionProperties.parse("sslmode=require").get("sslmode"));
        assertEquals("require", JdbcConnectionProperties.parse("sslmode=require", false).get("sslmode"));
        assertEquals("verify-full", JdbcConnectionProperties.parse("sslmode=verify-full").get("sslmode"));
    }

    public void testParseAllowsOpportunisticSslModes() {
        // prefer/allow are opportunistic defaults, not an explicit disable -- they must NOT be rejected.
        assertEquals("prefer", JdbcConnectionProperties.parse("sslmode=prefer").get("sslmode"));
        assertEquals("allow", JdbcConnectionProperties.parse("sslmode=allow").get("sslmode"));
    }

    // -- applyTo: precedence (typed credentials always win) ----------------------------------

    public void testApplyToNeverOverwritesTypedCredentials() {
        Properties props = new Properties();
        props.setProperty("user", "alice");
        props.setProperty("password", "secret");
        // Even a hand-built map that (bypassing parse) contained user/password must not clobber the typed values.
        JdbcConnectionProperties.applyTo(props, Map.of("ApplicationName", "es", "user", "mallory"));
        assertEquals("typed user must win", "alice", props.getProperty("user"));
        assertEquals("es", props.getProperty("ApplicationName"));
    }

    public void testApplyToEmptyIsNoOp() {
        Properties props = new Properties();
        JdbcConnectionProperties.applyTo(props, Map.of());
        assertTrue(props.isEmpty());
    }

    // -- end-to-end: allowlisted prop reaches the driver Properties --------------------------

    public void testAllowlistedPropertyFlowsToDriver() throws Exception {
        String jdbcUrl = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        try (Connection keepAlive = DriverManager.getConnection(jdbcUrl)) {
            try (var st = keepAlive.createStatement()) {
                st.execute("CREATE TABLE T (A INTEGER)");
                st.execute("INSERT INTO T VALUES (1)");
            }
            BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
                .breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST))
                .build();

            // Capture the Properties the driver would receive, but connect to H2 WITHOUT the extra props (H2 rejects
            // unknown connection settings) so this stays a pure "did it reach the driver layer" assertion.
            AtomicReference<Properties> captured = new AtomicReference<>();
            JdbcConnector.ConnectionSource capturingSource = (url, props) -> {
                captured.set((Properties) props.clone());
                return DriverManager.getConnection(jdbcUrl);
            };
            Map<String, String> connectionProperties = JdbcConnectionProperties.parse("ApplicationName=es-test;sslmode=require");
            JdbcConnector connector = new JdbcConnector(
                capturingSource,
                GenericDialect.INSTANCE,
                jdbcUrl,
                credentials("sa", "sapass"),
                connectionProperties,
                () -> 0L
            );
            try (ResultCursor cursor = connector.execute(request(blockFactory), (Split) null)) {
                assertNotNull(cursor);
            }
            Properties props = captured.get();
            assertNotNull("connection source must have been invoked", props);
            assertEquals("allowlisted prop must reach the driver", "es-test", props.getProperty("ApplicationName"));
            assertEquals("require", props.getProperty("sslmode"));
            // Typed credentials still present and un-clobbered.
            assertEquals("sa", props.getProperty("user"));
            assertEquals("sapass", props.getProperty("password"));
        }
    }

    private static JdbcConnector.CredentialSource credentials(String user, String password) {
        return new JdbcConnector.CredentialSource() {
            @Override
            public void writeInto(Properties props) {
                props.setProperty("user", user);
                props.setProperty("password", password);
            }

            @Override
            public boolean refreshable() {
                return false;
            }
        };
    }

    private static QueryRequest request(BlockFactory blockFactory) {
        Attribute a = new FieldAttribute(
            Source.EMPTY,
            "A",
            new EsField("A", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.UNKNOWN)
        );
        return new QueryRequest("t", List.of("A"), List.of(a), Map.of("table", "T"), 1024, 0, blockFactory);
    }
}
