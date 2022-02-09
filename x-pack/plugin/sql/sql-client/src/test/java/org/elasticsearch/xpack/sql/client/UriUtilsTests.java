/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.test.ESTestCase;

import java.net.URI;
import java.util.Arrays;

import static org.elasticsearch.xpack.sql.client.UriUtils.CredentialsRedaction.REDACTION_CHAR;
import static org.elasticsearch.xpack.sql.client.UriUtils.CredentialsRedaction.redactCredentialsInConnectionString;
import static org.elasticsearch.xpack.sql.client.UriUtils.appendSegmentToPath;
import static org.elasticsearch.xpack.sql.client.UriUtils.parseURI;
import static org.elasticsearch.xpack.sql.client.UriUtils.removeQuery;

public class UriUtilsTests extends ESTestCase {

    public static URI DEFAULT_URI = URI.create("http://localhost:9200/");

    public void testHostAndPort() throws Exception {
        assertEquals(URI.create("http://server:9200/"), parseURI("server:9200", DEFAULT_URI));
    }

    public void testJustHost() throws Exception {
        assertEquals(URI.create("http://server:9200/"), parseURI("server", DEFAULT_URI));
    }

    public void testHttpWithPort() throws Exception {
        assertEquals(URI.create("http://server:9201/"), parseURI("http://server:9201", DEFAULT_URI));
    }

    public void testHttpsWithPort() throws Exception {
        assertEquals(URI.create("https://server:9201/"), parseURI("https://server:9201", DEFAULT_URI));
    }

    public void testHttpNoPort() throws Exception {
        assertEquals(URI.create("https://server:9200/"), parseURI("https://server", DEFAULT_URI));
    }

    public void testLocalhostV6() throws Exception {
        assertEquals(URI.create("http://[::1]:51082/"), parseURI("[::1]:51082", DEFAULT_URI));
    }

    public void testUserLocalhostV6() throws Exception {
        assertEquals(URI.create("http://user@[::1]:51082/"), parseURI("user@[::1]:51082", DEFAULT_URI));
    }

    public void testLocalhostV4() throws Exception {
        assertEquals(URI.create("http://127.0.0.1:51082/"), parseURI("127.0.0.1:51082", DEFAULT_URI));
    }

    public void testUserLocalhostV4() throws Exception {
        assertEquals(URI.create("http://user@127.0.0.1:51082/"), parseURI("user@127.0.0.1:51082", DEFAULT_URI));
    }

    public void testNoHost() {
        assertEquals(URI.create("http://localhost:9200/path?query"), parseURI("/path?query", DEFAULT_URI));
    }

    public void testEmpty() {
        assertEquals(URI.create("http://localhost:9200/"), parseURI("", DEFAULT_URI));
    }

    public void testHttpsWithUser() throws Exception {
        assertEquals(URI.create("https://user@server:9200/"), parseURI("https://user@server", DEFAULT_URI));
    }

    public void testUserPassHost() throws Exception {
        assertEquals(URI.create("http://user:password@server:9200/"), parseURI("user:password@server", DEFAULT_URI));
    }

    public void testHttpPath() throws Exception {
        assertEquals(URI.create("https://server:9201/some_path"), parseURI("https://server:9201/some_path", DEFAULT_URI));
    }

    public void testHttpQuery() throws Exception {
        assertEquals(URI.create("https://server:9201/?query"), parseURI("https://server:9201/?query", DEFAULT_URI));
    }

    public void testUnsupportedProtocol() throws Exception {
        assertEquals(
            "Invalid connection scheme [ftp] configuration: only http and https protocols are supported",
            expectThrows(IllegalArgumentException.class, () -> parseURI("ftp://server:9201/", DEFAULT_URI)).getMessage()
        );
    }

    public void testMalformedWhiteSpace() throws Exception {
        assertEquals(
            "Invalid connection configuration: Illegal character in authority at index 7: http:// ",
            expectThrows(IllegalArgumentException.class, () -> parseURI(" ", DEFAULT_URI)).getMessage()
        );
    }

    public void testNoRedaction() {
        assertEquals(
            "Invalid connection configuration: Illegal character in fragment at index 16: HTTP://host#frag#ment",
            expectThrows(IllegalArgumentException.class, () -> parseURI("HTTP://host#frag#ment", DEFAULT_URI)).getMessage()
        );
    }

    public void testSimpleUriRedaction() {
        assertEquals(
            "http://*************@host:9200/path?user=****&password=****",
            redactCredentialsInConnectionString("http://user:password@host:9200/path?user=user&password=pass")
        );
    }

    public void testSimpleConnectionStringRedaction() {
        assertEquals(
            "*************@host:9200/path?user=****&password=****",
            redactCredentialsInConnectionString("user:password@host:9200/path?user=user&password=pass")
        );
    }

    public void testNoRedactionInvalidHost() {
        assertEquals("https://ho%st", redactCredentialsInConnectionString("https://ho%st"));
    }

    public void testUriRedactionInvalidUserPart() {
        assertEquals(
            "http://*************@@host:9200/path?user=****&password=****&at=@sign",
            redactCredentialsInConnectionString("http://user:password@@host:9200/path?user=user&password=pass&at=@sign")
        );
    }

    public void testUriRedactionInvalidHost() {
        assertEquals(
            "http://*************@ho%st:9200/path?user=****&password=****&at=@sign",
            redactCredentialsInConnectionString("http://user:password@ho%st:9200/path?user=user&password=pass&at=@sign")
        );
    }

    public void testUriRedactionInvalidPort() {
        assertEquals(
            "http://*************@host:port/path?user=****&password=****&at=@sign",
            redactCredentialsInConnectionString("http://user:password@host:port/path?user=user&password=pass&at=@sign")
        );
    }

    public void testUriRedactionInvalidPath() {
        assertEquals(
            "http://*************@host:9200/pa^th?user=****&password=****",
            redactCredentialsInConnectionString("http://user:password@host:9200/pa^th?user=user&password=pass")
        );
    }

    public void testUriRedactionInvalidQuery() {
        assertEquals(
            "http://*************@host:9200/path?user=****&password=****&invali^d",
            redactCredentialsInConnectionString("http://user:password@host:9200/path?user=user&password=pass&invali^d")
        );
    }

    public void testUriRedactionInvalidFragment() {
        assertEquals(
            "https://host:9200/path?usr=****&passwo=****#ssl=5#",
            redactCredentialsInConnectionString("https://host:9200/path?usr=user&passwo=pass#ssl=5#")
        );
    }

    public void testUriRedactionMisspelledUser() {
        assertEquals(
            "https://host:9200/path?usr=****&password=****",
            redactCredentialsInConnectionString("https://host:9200/path?usr=user&password=pass")
        );
    }

    public void testUriRedactionMisspelledUserAndPassword() {
        assertEquals(
            "https://host:9200/path?usr=****&passwo=****",
            redactCredentialsInConnectionString("https://host:9200/path?usr=user&passwo=pass")
        );
    }

    public void testUriRedactionNoScheme() {
        assertEquals("host:9200/path?usr=****&passwo=****", redactCredentialsInConnectionString("host:9200/path?usr=user&passwo=pass"));
    }

    public void testUriRedactionNoPort() {
        assertEquals("host/path?usr=****&passwo=****", redactCredentialsInConnectionString("host/path?usr=user&passwo=pass"));
    }

    public void testUriRedactionNoHost() {
        assertEquals("/path?usr=****&passwo=****", redactCredentialsInConnectionString("/path?usr=user&passwo=pass"));
    }

    public void testUriRedactionNoPath() {
        assertEquals("?usr=****&passwo=****", redactCredentialsInConnectionString("?usr=user&passwo=pass"));
    }

    public void testUriRandomRedact() {
        final String scheme = randomFrom("http://", "https://", StringUtils.EMPTY);
        final String host = randomFrom("host", "host:" + randomIntBetween(1, 65535), StringUtils.EMPTY);
        final String path = randomFrom("", "/", "/path", StringUtils.EMPTY);
        final String userVal = randomAlphaOfLengthBetween(1, 2 + randomInt(50));
        final String user = "user=" + userVal;
        final String passVal = randomAlphaOfLengthBetween(1, 2 + randomInt(50));
        final String pass = "password=" + passVal;
        final String redactedUser = "user=" + String.valueOf(REDACTION_CHAR).repeat(userVal.length());
        final String redactedPass = "password=" + String.valueOf(REDACTION_CHAR).repeat(passVal.length());

        String connStr, expectRedact, expectParse, creds = StringUtils.EMPTY;
        if (randomBoolean() && host.length() > 0) {
            creds = userVal;
            if (randomBoolean()) {
                creds += ":" + passVal;
            }
            connStr = scheme + creds + "@" + host + path;
            expectRedact = scheme + String.valueOf(REDACTION_CHAR).repeat(creds.length()) + "@" + host + path;
        } else {
            connStr = scheme + host + path;
            expectRedact = scheme + host + path;
        }

        expectParse = scheme.length() > 0 ? scheme : "http://";
        expectParse += creds + (creds.length() > 0 ? "@" : StringUtils.EMPTY);
        expectParse += host.length() > 0 ? host : "localhost";
        expectParse += (host.indexOf(':') > 0 ? StringUtils.EMPTY : ":" + 9200);
        expectParse += path.length() > 0 ? path : "/";

        Character sep = '?';
        if (randomBoolean()) {
            connStr += sep + user;
            expectRedact += sep + redactedUser;
            expectParse += sep + user;
            sep = '&';
        }
        if (randomBoolean()) {
            connStr += sep + pass;
            expectRedact += sep + redactedPass;
            expectParse += sep + pass;
        }

        assertEquals(expectRedact, redactCredentialsInConnectionString(connStr));
        if ((connStr.equals("http://") || connStr.equals("https://")) == false) { // URI parser expects an authority past a scheme
            assertEquals(URI.create(expectParse), parseURI(connStr, DEFAULT_URI));
        }
    }

    public void testUriRedactionMissingSeparatorBetweenUserAndPassword() {
        assertEquals(
            "https://host:9200/path?user=*****************",
            redactCredentialsInConnectionString("https://host:9200/path?user=userpassword=pass")
        );
    }

    public void testUriRedactionMissingSeparatorBeforePassword() {
        assertEquals(
            "https://host:9200/path?user=****&foo=barpassword=********&bar=foo",
            redactCredentialsInConnectionString("https://host:9200/path?user=user&foo=barpassword=password&bar=foo")
        );
    }

    // tests that no other option is "similar" to the credential options and be inadvertently redacted
    public void testUriRedactionAllOptions() {
        StringBuilder cs = new StringBuilder("https://host:9200/path?");
        String[] options = {
            "timezone",
            "connect.timeout",
            "network.timeout",
            "page.timeout",
            "page.size",
            "query.timeout",
            "user",
            "password",
            "ssl",
            "ssl.keystore.location",
            "ssl.keystore.pass",
            "ssl.keystore.type",
            "ssl.truststore.location",
            "ssl.truststore.pass",
            "ssl.truststore.type",
            "ssl.protocol",
            "proxy.http",
            "proxy.socks",
            "field.multi.value.leniency",
            "index.include.frozen",
            "validate.properties" };
        Arrays.stream(options).forEach(e -> cs.append(e).append("=").append(e).append("&"));
        String connStr = cs.substring(0, cs.length() - 1);
        String expected = connStr.replace("user=user", "user=****");
        expected = expected.replace("password=password", "password=********");
        assertEquals(expected, redactCredentialsInConnectionString(connStr));
    }

    public void testUriRedactionBrokenHost() {
        assertEquals("ho^st", redactCredentialsInConnectionString("ho^st"));
    }

    public void testUriRedactionDisabled() {
        assertEquals(
            "HTTPS://host:9200/path?user=user;password=pass",
            redactCredentialsInConnectionString("HTTPS://host:9200/path?user=user;password=pass")
        );
    }

    public void testRemoveQuery() throws Exception {
        assertEquals(
            URI.create("http://server:9100"),
            removeQuery(URI.create("http://server:9100?query"), "http://server:9100?query", DEFAULT_URI)
        );
    }

    public void testRemoveQueryTrailingSlash() throws Exception {
        assertEquals(
            URI.create("http://server:9100/"),
            removeQuery(URI.create("http://server:9100/?query"), "http://server:9100/?query", DEFAULT_URI)
        );
    }

    public void testRemoveQueryNoQuery() throws Exception {
        assertEquals(URI.create("http://server:9100"), removeQuery(URI.create("http://server:9100"), "http://server:9100", DEFAULT_URI));
    }

    public void testAppendEmptySegmentToPath() throws Exception {
        assertEquals(URI.create("http://server:9100"), appendSegmentToPath(URI.create("http://server:9100"), ""));
    }

    public void testAppendNullSegmentToPath() throws Exception {
        assertEquals(URI.create("http://server:9100"), appendSegmentToPath(URI.create("http://server:9100"), null));
    }

    public void testAppendSegmentToNullPath() throws Exception {
        assertEquals(
            "URI must not be null",
            expectThrows(IllegalArgumentException.class, () -> appendSegmentToPath(null, "/_sql")).getMessage()
        );
    }

    public void testAppendSegmentToEmptyPath() throws Exception {
        assertEquals(URI.create("/_sql"), appendSegmentToPath(URI.create(""), "/_sql"));
    }

    public void testAppendSlashSegmentToPath() throws Exception {
        assertEquals(URI.create("http://server:9100"), appendSegmentToPath(URI.create("http://server:9100"), "/"));
    }

    public void testAppendSqlSegmentToPath() throws Exception {
        assertEquals(URI.create("http://server:9100/_sql"), appendSegmentToPath(URI.create("http://server:9100"), "/_sql"));
    }

    public void testAppendSqlSegmentNoSlashToPath() throws Exception {
        assertEquals(URI.create("http://server:9100/_sql"), appendSegmentToPath(URI.create("http://server:9100"), "_sql"));
    }

    public void testAppendSegmentToPath() throws Exception {
        assertEquals(URI.create("http://server:9100/es_rest/_sql"), appendSegmentToPath(URI.create("http://server:9100/es_rest"), "/_sql"));
    }

    public void testAppendSegmentNoSlashToPath() throws Exception {
        assertEquals(URI.create("http://server:9100/es_rest/_sql"), appendSegmentToPath(URI.create("http://server:9100/es_rest"), "_sql"));
    }

    public void testAppendSegmentTwoSlashesToPath() throws Exception {
        assertEquals(
            URI.create("https://server:9100/es_rest/_sql"),
            appendSegmentToPath(URI.create("https://server:9100/es_rest/"), "/_sql")
        );
    }
}
