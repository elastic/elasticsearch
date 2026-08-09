/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;

/**
 * Verifies that {@link JdbcUrlSanitizer#sanitize} actually removes credentials from every shape of JDBC URL we
 * have seen in the wild. Each test is a plausible URL that would otherwise leak through the previous
 * {@code indexOf('?')}-only sanitization.
 */
public class JdbcUrlSanitizerTests extends ESTestCase {

    public void testStripsUserinfoBeforeAt() {
        String redacted = JdbcUrlSanitizer.sanitize("jdbc:postgresql://alice:s3cret@host:5432/db");
        assertEquals("jdbc:postgresql://REDACTED@host:5432/db", redacted);
        assertFalse("password must not survive", redacted.contains("s3cret"));
        assertFalse("user must not survive in userinfo", redacted.contains("alice"));
    }

    public void testStripsOracleThinUserinfo() {
        // Oracle thin form: jdbc:oracle:thin:user/pass@host:port:sid -- no '//' authority.
        String redacted = JdbcUrlSanitizer.sanitize("jdbc:oracle:thin:scott/tiger@db.example.com:1521:orcl");
        assertFalse("password must not survive", redacted.contains("tiger"));
        assertFalse("user must not survive", redacted.contains("scott"));
        assertTrue("host should remain", redacted.contains("db.example.com"));
        assertTrue("port should remain", redacted.contains("1521"));
        assertTrue("sid should remain", redacted.contains("orcl"));
    }

    public void testDropsQueryString() {
        String redacted = JdbcUrlSanitizer.sanitize("jdbc:mysql://host:3306/db?user=root&password=hunter2");
        assertEquals("jdbc:mysql://host:3306/db", redacted);
        assertFalse(redacted.contains("hunter2"));
    }

    public void testRedactsSemicolonProperties() {
        // SQL Server style: jdbc:sqlserver://host;user=sa;password=foo;encrypt=true
        String redacted = JdbcUrlSanitizer.sanitize("jdbc:sqlserver://host:1433;user=sa;password=foo;encrypt=true");
        assertFalse("password must not survive", redacted.contains("foo"));
        assertFalse("user must not survive", redacted.contains(";user=sa"));
        assertTrue("non-credential property survives", redacted.contains("encrypt=true"));
        assertTrue("REDACTED placeholder is present", redacted.contains("REDACTED"));
    }

    public void testCaseInsensitiveCredentialKeys() {
        String redacted = JdbcUrlSanitizer.sanitize("jdbc:sqlserver://host;USER=sa;PASSWORD=foo;PWD=bar");
        assertFalse(redacted.contains("foo"));
        assertFalse(redacted.contains("bar"));
    }

    public void testRedactsAwsCredentialKeysInMessages() {
        // A driver that echoes the explicit AWS credentials in its exception text must have them
        // scrubbed. Both the driver-property casing and the connector config-key casing are covered.
        String msg = "auth failed AccessKeyID=AKIALEAK SecretAccessKey=wJaLEAK SessionToken=FQoLEAK access_key_id=snakeLEAK";
        String scrubbed = JdbcUrlSanitizer.sanitizeMessage(msg);
        assertFalse("access key must not survive", scrubbed.contains("AKIALEAK"));
        assertFalse("secret key must not survive", scrubbed.contains("wJaLEAK"));
        assertFalse("session token must not survive", scrubbed.contains("FQoLEAK"));
        assertFalse("snake_case config key must not survive", scrubbed.contains("snakeLEAK"));
        assertTrue("keys are redacted in place", scrubbed.contains("AccessKeyID=REDACTED"));
    }

    public void testAwsCredentialKeysAreCredentialKeys() {
        // The single source of truth used by the connection_properties passthrough to reject secret AWS keys.
        assertTrue(JdbcUrlSanitizer.credentialKeys().contains("accesskeyid"));
        assertTrue(JdbcUrlSanitizer.credentialKeys().contains("secretaccesskey"));
        assertTrue(JdbcUrlSanitizer.credentialKeys().contains("sessiontoken"));
        assertTrue(JdbcUrlSanitizer.credentialKeys().contains("access_key_id"));
    }

    public void testHandlesUrlWithoutCredentials() {
        String url = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
        assertEquals(url, JdbcUrlSanitizer.sanitize(url));
    }

    public void testHandlesNullAndEmpty() {
        assertNull(JdbcUrlSanitizer.sanitize(null));
        assertEquals("", JdbcUrlSanitizer.sanitize(""));
    }

    public void testStripsCredentialsAndQueryAndProperties() {
        // Combined worst-case URL.
        String url = "jdbc:postgresql://alice:s3cret@host:5432/db;applicationName=esql;password=otherSecret?user=bob&password=q";
        String redacted = JdbcUrlSanitizer.sanitize(url);
        assertFalse("URL userinfo must be redacted", redacted.contains("alice"));
        assertFalse("URL userinfo password must be redacted", redacted.contains("s3cret"));
        assertFalse("query string must be dropped (with both user and password)", redacted.contains("?"));
        assertFalse("semicolon password must be redacted", redacted.contains("otherSecret"));
        assertTrue("non-credential property survives", redacted.contains("applicationName=esql"));
    }

    public void testPreservesCaseOfHostAndDb() {
        // The previous sanitizer lowercased the URL, mangling case-sensitive identifiers in error messages. Verify
        // case is preserved.
        String redacted = JdbcUrlSanitizer.sanitize("jdbc:postgresql://Host.Example.COM/MyDB");
        assertTrue(redacted.contains("Host.Example.COM"));
        assertTrue(redacted.contains("MyDB"));
    }

    // -- sanitizeMessage: free-form text from JDBC driver exceptions --

    public void testSanitizeMessageRedactsUrlUserinfoInProse() {
        // Postgres-style: the driver embeds the connection URL in its exception text.
        String redacted = JdbcUrlSanitizer.sanitizeMessage(
            "Connection refused for jdbc:postgresql://alice:s3cret@host:5432/db (recheck firewall)"
        );
        assertFalse("password must not survive", redacted.contains("s3cret"));
        assertFalse("user must not survive in userinfo", redacted.contains("alice"));
        assertTrue("the rest of the message must remain", redacted.contains("recheck firewall"));
        assertTrue("host must remain", redacted.contains("host:5432"));
    }

    public void testSanitizeMessageRedactsLooseCredentialKvPairs() {
        // SQL Server-style and some loose property formats.
        String redacted = JdbcUrlSanitizer.sanitizeMessage("Login failed: user=sa; password=hunter2; database=master");
        assertFalse(redacted.contains("hunter2"));
        assertFalse(redacted.contains("user=sa"));
        assertTrue("non-credential property survives", redacted.contains("database=master"));
    }

    public void testSanitizeMessageHandlesPasswordEqualsInsideText() {
        // Even when the kv pair is embedded mid-sentence ("setting password=foo failed").
        String redacted = JdbcUrlSanitizer.sanitizeMessage("setting password=foo failed");
        assertFalse(redacted.contains("=foo"));
        assertTrue(redacted.contains("REDACTED"));
    }

    public void testSanitizeMessageRedactsRealisticPasswordChars() {
        // Realistic passwords contain non-identifier characters: %, !, $, ~, =, (, ), etc. The earlier strict regex
        // missed these. The jdbc:-token path passes through sanitize() which locates userinfo by '@' position, not
        // by character class.
        for (String password : new String[] { "p%40ss!", "p$ssw0rd~", "a=b!c%d", "P@ssw(rd)" }) {
            String url = "jdbc:postgresql://alice:" + password + "@host:5432/db";
            String redacted = JdbcUrlSanitizer.sanitizeMessage("connect failed for " + url);
            assertFalse("password [" + password + "] must not survive in [" + redacted + "]", redacted.contains(password));
            assertFalse("user must not survive in [" + redacted + "]", redacted.contains("alice"));
            assertTrue("the rest of the message must remain", redacted.contains("connect failed for"));
            assertTrue("host must remain", redacted.contains("host:5432"));
        }
    }

    public void testSanitizeMessageRedactsJdbcUrlInsidePunctuation() {
        // Driver text often wraps the URL in parentheses or quotes. Our token matcher must stop at those delimiters
        // so we don't over-eat into the surrounding prose.
        String redacted = JdbcUrlSanitizer.sanitizeMessage("Connection (jdbc:postgresql://alice:p%40ss!@host/db) refused: see logs");
        assertFalse(redacted.contains("p%40ss!"));
        assertFalse(redacted.contains("alice"));
        assertTrue("trailing prose must survive", redacted.contains("refused: see logs"));
        assertTrue("opening parenthesis must survive", redacted.contains("Connection ("));
    }

    public void testSanitizeMessageHandlesQueryStringInJdbcUrlToken() {
        // jdbc:foo://host?password=xxx&user=bob -- the token sanitizer drops the entire query string.
        String redacted = JdbcUrlSanitizer.sanitizeMessage("auth failed: jdbc:mysql://host:3306/db?user=bob&password=xxx");
        assertFalse(redacted.contains("password=xxx"));
        assertFalse(redacted.contains("user=bob"));
    }

    public void testSanitizeMessageNullReturnsNull() {
        assertNull(JdbcUrlSanitizer.sanitizeMessage(null));
    }

    public void testSanitizeMessageEmptyReturnsEmpty() {
        assertEquals("", JdbcUrlSanitizer.sanitizeMessage(""));
    }

    public void testSanitizeMessagePreservesNonCredentialContent() {
        // Plain error message with no credentials must come through unchanged.
        String message = "Table PEOPLE not found; SQLState [42S02]";
        assertEquals(message, JdbcUrlSanitizer.sanitizeMessage(message));
    }

    // -- sanitize(SQLException): cause chain redaction --

    public void testSanitizeSqlExceptionRedactsMessageAndPreservesSqlState() {
        java.sql.SQLException original = new java.sql.SQLException("auth failed for jdbc:mysql://alice:s3cret@host/db", "28000", 1045);
        java.sql.SQLException sanitized = JdbcUrlSanitizer.sanitizeException(original);
        assertNotSame("must return a fresh exception", original, sanitized);
        assertEquals("SQLState must survive", "28000", sanitized.getSQLState());
        assertEquals("vendor error code must survive", 1045, sanitized.getErrorCode());
        assertFalse("password must not survive", sanitized.getMessage().contains("s3cret"));
        assertFalse("user must not survive", sanitized.getMessage().contains("alice"));
    }

    public void testSanitizeSqlExceptionWalksCauseChain() {
        // Driver wraps a network exception inside a SQLException; both contain credentials.
        java.io.IOException root = new java.io.IOException("connect to alice:s3cret@host:5432 timed out");
        java.sql.SQLException middle = new java.sql.SQLException("password=hunter2 rejected", "08001", 0, root);
        java.sql.SQLException original = new java.sql.SQLException("connection failed", "08000", 0, middle);
        java.sql.SQLException sanitized = JdbcUrlSanitizer.sanitizeException(original);
        // Walk the cause chain on the sanitized side and verify no link mentions any of the secrets.
        Throwable t = sanitized;
        while (t != null) {
            String m = t.getMessage();
            if (m != null) {
                assertFalse("hunter2 must not appear in cause chain message: " + m, m.contains("hunter2"));
                assertFalse("s3cret must not appear in cause chain message: " + m, m.contains("s3cret"));
                assertFalse("alice must not appear in cause chain message: " + m, m.contains("alice"));
            }
            t = t.getCause();
        }
    }

    public void testSanitizeSqlExceptionHandlesNullInput() {
        assertNull(JdbcUrlSanitizer.sanitizeException(null));
    }

    public void testSanitizeSqlExceptionTerminatesOnCycle() {
        // Construct a self-referential cause chain (pathological). Sanitizer must not spin.
        java.sql.SQLException a = new java.sql.SQLException("a");
        java.sql.SQLException b = new java.sql.SQLException("b");
        a.initCause(b);
        // Reflection-free cycle: re-raise b's cause to a via a non-SQL-Exception link to satisfy initCause's
        // "self-cause" check.
        Throwable spoof = new RuntimeException("loop", a);
        b.initCause(spoof);
        // Should return without throwing or hanging.
        java.sql.SQLException sanitized = JdbcUrlSanitizer.sanitizeException(a);
        assertNotNull(sanitized);
    }
}
