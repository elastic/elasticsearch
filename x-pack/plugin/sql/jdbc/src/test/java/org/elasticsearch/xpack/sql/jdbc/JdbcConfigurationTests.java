/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.client.SslConfig;
import org.elasticsearch.xpack.sql.client.SuppressForbidden;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.sql.client.ConnectionConfiguration.CONNECT_TIMEOUT;
import static org.elasticsearch.xpack.sql.client.ConnectionConfiguration.NETWORK_TIMEOUT;
import static org.elasticsearch.xpack.sql.client.ConnectionConfiguration.PAGE_SIZE;
import static org.elasticsearch.xpack.sql.client.ConnectionConfiguration.PAGE_TIMEOUT;
import static org.elasticsearch.xpack.sql.client.ConnectionConfiguration.PROPERTIES_VALIDATION;
import static org.elasticsearch.xpack.sql.client.ConnectionConfiguration.QUERY_TIMEOUT;
import static org.elasticsearch.xpack.sql.jdbc.JdbcConfiguration.URL_FULL_PREFIX;
import static org.elasticsearch.xpack.sql.jdbc.JdbcConfiguration.URL_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class JdbcConfigurationTests extends ESTestCase {

    private JdbcConfiguration ci(String url) throws SQLException {
        return JdbcConfiguration.create(url, null, 0);
    }

    public void testInvalidUrl() {
        JdbcSQLException e = expectThrows(JdbcSQLException.class, () -> ci("jdbc:es://localhost9200/?ssl=#5#"));
        assertEquals("Invalid URL: Invalid connection configuration: Illegal character in fragment at index 28: "
            + "http://localhost9200/?ssl=#5#; format should be "
            + "[jdbc:[es|elasticsearch]://[[http|https]://]?[host[:port]]?/[prefix]?[\\?[option=value]&]*]", e.getMessage());
    }

    public void testJustThePrefix() throws Exception {
        Exception e = expectThrows(JdbcSQLException.class, () -> ci("jdbc:es:"));
        assertEquals("Expected [jdbc:es://] url, received [jdbc:es:]", e.getMessage());
    }

    public void testJustTheHost() throws Exception {
        assertThat(ci("jdbc:es://localhost").baseUri().toString(), is("http://localhost:9200/"));
        assertThat(ci("jdbc:elasticsearch://localhost").baseUri().toString(), is("http://localhost:9200/"));
    }

    private static String jdbcPrefix() {
        return randomFrom(URL_PREFIX, URL_FULL_PREFIX);
    }

    public void testHostAndPort() throws Exception {
        assertThat(ci(jdbcPrefix() + "localhost:1234").baseUri().toString(), is("http://localhost:1234/"));
    }

    public void testPropertiesEscaping() throws Exception {
        String pass = randomUnicodeOfLengthBetween(1, 500);
        String encPass = URLEncoder.encode(pass, StandardCharsets.UTF_8).replace("+", "%20");
        String url = jdbcPrefix() + "test?password=" + encPass;
        JdbcConfiguration ci = ci(url);
        assertEquals(pass, ci.authPass());
    }

    public void testTrailingSlashForHost() throws Exception {
        assertThat(ci(jdbcPrefix() + "localhost:1234/").baseUri().toString(), is("http://localhost:1234/"));
    }

    public void testMultiPathSuffix() throws Exception {
        assertThat(ci(jdbcPrefix() + "a:1/foo/bar/tar").baseUri().toString(), is("http://a:1/foo/bar/tar"));
    }

    public void testV6Localhost() throws Exception {
        assertThat(ci(jdbcPrefix() + "[::1]:54161/foo/bar").baseUri().toString(), is("http://[::1]:54161/foo/bar"));
    }

    public void testDebug() throws Exception {
        JdbcConfiguration ci = ci(jdbcPrefix() + "a:1/?debug=true");
        assertThat(ci.baseUri().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("err"));
    }

    public void testDebugOut() throws Exception {
        JdbcConfiguration ci = ci(jdbcPrefix() + "a:1/?debug=true&debug.output=jdbc.out");
        assertThat(ci.baseUri().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("jdbc.out"));
    }

    public void testDebugFlushAlways() throws Exception {
        JdbcConfiguration ci = ci(jdbcPrefix() + "a:1/?debug=true&debug.flushAlways=false");
        assertThat(ci.baseUri().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.flushAlways(), is(false));

        ci = ci(jdbcPrefix() + "a:1/?debug=true&debug.flushAlways=true");
        assertThat(ci.baseUri().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.flushAlways(), is(true));

        ci = ci(jdbcPrefix() + "a:1/?debug=true");
        assertThat(ci.baseUri().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.flushAlways(), is(false));
    }

    public void testTypeInParam() throws Exception {
        Exception e = expectThrows(JdbcSQLException.class, () -> ci(jdbcPrefix() + "a:1/foo/bar/tar?debug=true&debug.out=jdbc.out"));
        assertEquals("Unknown parameter [debug.out]; did you mean [debug.output]", e.getMessage());
    }

    public void testDebugOutWithSuffix() throws Exception {
        JdbcConfiguration ci = ci(jdbcPrefix() + "a:1/foo/bar/tar?debug=true&debug.output=jdbc.out");
        assertThat(ci.baseUri().toString(), is("http://a:1/foo/bar/tar"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("jdbc.out"));
    }

    public void testHttpWithSSLEnabledFromProperty() throws Exception {
        JdbcConfiguration ci = ci(jdbcPrefix() + "test?ssl=true");
        assertThat(ci.baseUri().toString(), is("https://test:9200/"));
    }

    public void testHttpWithSSLEnabledFromPropertyAndDisabledFromProtocol() throws Exception {
        JdbcConfiguration ci = ci(jdbcPrefix() + "http://test?ssl=true");
        assertThat(ci.baseUri().toString(), is("https://test:9200/"));
    }

    public void testHttpWithSSLEnabledFromProtocol() throws Exception {
        JdbcConfiguration ci = ci(jdbcPrefix() + "https://test:9200");
        assertThat(ci.baseUri().toString(), is("https://test:9200/"));
    }

    public void testHttpWithSSLEnabledFromProtocolAndProperty() throws Exception {
        JdbcConfiguration ci = ci(jdbcPrefix() + "https://test:9200?ssl=true");
        assertThat(ci.baseUri().toString(), is("https://test:9200/"));
    }

    public void testHttpWithSSLDisabledFromProperty() throws Exception {
        JdbcConfiguration ci = ci(jdbcPrefix() + "test?ssl=false");
        assertThat(ci.baseUri().toString(), is("http://test:9200/"));
    }

    public void testHttpWithSSLDisabledFromPropertyAndProtocol() throws Exception {
        JdbcConfiguration ci = ci(jdbcPrefix() + "http://test?ssl=false");
        assertThat(ci.baseUri().toString(), is("http://test:9200/"));
    }

    public void testHttpWithSSLDisabledFromPropertyAndEnabledFromProtocol() throws Exception {
        Exception e = expectThrows(JdbcSQLException.class, () -> ci(jdbcPrefix() + "https://test?ssl=false"));
        assertEquals("Cannot enable SSL: HTTPS protocol being used in the URL and SSL disabled in properties", e.getMessage());
    }

    public void testValidatePropertiesDefault() {
        Exception e = expectThrows(JdbcSQLException.class, () -> ci(jdbcPrefix() + "test:9200?pagee.size=12"));
        assertEquals("Unknown parameter [pagee.size]; did you mean [page.size]", e.getMessage());

        e = expectThrows(JdbcSQLException.class, () -> ci(jdbcPrefix() + "test:9200?foo=bar"));
        assertEquals("Unknown parameter [foo]; did you mean [ssl]", e.getMessage());
    }

    public void testValidateProperties() {
        Exception e = expectThrows(JdbcSQLException.class, () -> ci(jdbcPrefix() + "test:9200?pagee.size=12&validate.properties=true"));
        assertEquals("Unknown parameter [pagee.size]; did you mean [page.size]", e.getMessage());

        e = expectThrows(JdbcSQLException.class, () -> ci(jdbcPrefix() + "test:9200?&validate.properties=true&something=some_value"));
        assertEquals("Unknown parameter [something]; did you mean []", e.getMessage());

        Properties properties  = new Properties();
        properties.setProperty(PROPERTIES_VALIDATION, "true");
        e = expectThrows(JdbcSQLException.class,
            () -> JdbcConfiguration.create(jdbcPrefix() + "test:9200?something=some_value", properties, 0));
        assertEquals("Unknown parameter [something]; did you mean []", e.getMessage());
    }

    public void testNoPropertiesValidation() throws SQLException {
        JdbcConfiguration ci = ci(jdbcPrefix() + "test:9200?pagee.size=12&validate.properties=false");
        assertEquals(false, ci.validateProperties());

        // URL properties test
        long queryTimeout = randomNonNegativeLong();
        long connectTimeout = randomNonNegativeLong();
        long networkTimeout = randomNonNegativeLong();
        long pageTimeout = randomNonNegativeLong();
        int pageSize = randomIntBetween(0, Integer.MAX_VALUE);

        ci = ci(jdbcPrefix() + "test:9200?validate.properties=false&something=some_value&query.timeout=" + queryTimeout
                + "&connect.timeout=" + connectTimeout + "&network.timeout=" + networkTimeout + "&page.timeout=" + pageTimeout
                + "&page.size=" + pageSize);
        assertEquals(false, ci.validateProperties());
        assertEquals(queryTimeout, ci.queryTimeout());
        assertEquals(connectTimeout, ci.connectTimeout());
        assertEquals(networkTimeout, ci.networkTimeout());
        assertEquals(pageTimeout, ci.pageTimeout());
        assertEquals(pageSize, ci.pageSize());

        // Properties test
        Properties properties  = new Properties();
        properties.setProperty(PROPERTIES_VALIDATION, "false");
        properties.put(QUERY_TIMEOUT, Long.toString(queryTimeout));
        properties.put(PAGE_TIMEOUT, Long.toString(pageTimeout));
        properties.put(CONNECT_TIMEOUT, Long.toString(connectTimeout));
        properties.put(NETWORK_TIMEOUT, Long.toString(networkTimeout));
        properties.put(PAGE_SIZE, Integer.toString(pageSize));

        // also putting validate.properties in URL to be overriden by the properties value
        ci = JdbcConfiguration.create(jdbcPrefix() + "test:9200?validate.properties=true&something=some_value", properties, 0);
        assertEquals(false, ci.validateProperties());
        assertEquals(queryTimeout, ci.queryTimeout());
        assertEquals(connectTimeout, ci.connectTimeout());
        assertEquals(networkTimeout, ci.networkTimeout());
        assertEquals(pageTimeout, ci.pageTimeout());
        assertEquals(pageSize, ci.pageSize());
    }

    public void testTimoutOverride() throws Exception {
        Properties properties  = new Properties();
        properties.setProperty(CONNECT_TIMEOUT, "3"); // Should be overridden
        properties.setProperty(PAGE_TIMEOUT, "4");

        String url = jdbcPrefix() + "test?connect.timeout=1&page.timeout=2";

        // No properties
        JdbcConfiguration ci = JdbcConfiguration.create(url, null, 0);
        assertThat(ci.connectTimeout(), equalTo(1L));
        assertThat(ci.pageTimeout(), equalTo(2L));

        // Properties override
        ci = JdbcConfiguration.create(url, properties, 0);
        assertThat(ci.connectTimeout(), equalTo(3L));
        assertThat(ci.pageTimeout(), equalTo(4L));

        // Driver default override for connection timeout
        ci = JdbcConfiguration.create(url, properties, 5);
        assertThat(ci.connectTimeout(), equalTo(5000L));
        assertThat(ci.pageTimeout(), equalTo(4L));
    }

    public void testSSLPropertiesInUrl() throws Exception {
        Map<String, String> urlPropMap = sslProperties();

        Properties allProps = new Properties();
        allProps.putAll(urlPropMap);
        String sslUrlProps = urlPropMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("&"));

        assertSslConfig(allProps, ci(jdbcPrefix() + "test?" + sslUrlProps.toString()).sslConfig());
    }

    public void testSSLPropertiesInUrlAndProperties() throws Exception {
        Map<String, String> urlPropMap = new HashMap<>(4);
        urlPropMap.put("ssl", "false");
        urlPropMap.put("ssl.protocol", "SSLv3");
        urlPropMap.put("ssl.keystore.location", "/abc/xyz");
        urlPropMap.put("ssl.keystore.pass", "mypass");

        Map<String, String> propMap = new HashMap<>(4);
        propMap.put("ssl.keystore.type", "PKCS12");
        propMap.put("ssl.truststore.location", "/foo/bar");
        propMap.put("ssl.truststore.pass", "anotherpass");
        propMap.put("ssl.truststore.type", "jks");

        Properties props = new Properties();
        props.putAll(propMap);
        String sslUrlProps = urlPropMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("&"));

        Properties allProps = new Properties();
        allProps.putAll(urlPropMap);
        allProps.putAll(propMap);
        assertSslConfig(allProps, JdbcConfiguration.create(jdbcPrefix() + "test?" + sslUrlProps.toString(), props, 0).sslConfig());
    }

    public void testSSLPropertiesOverride() throws Exception {
        Map<String, String> urlPropMap = sslProperties();
        Map<String, String> propMap = new HashMap<>(8);
        propMap.put("ssl", "false");
        propMap.put("ssl.protocol", "TLS");
        propMap.put("ssl.keystore.location", "/xyz");
        propMap.put("ssl.keystore.pass", "different_mypass");
        propMap.put("ssl.keystore.type", "JKS");
        propMap.put("ssl.truststore.location", "/baz");
        propMap.put("ssl.truststore.pass", "different_anotherpass");
        propMap.put("ssl.truststore.type", "PKCS11");

        Properties props = new Properties();
        props.putAll(propMap);
        String sslUrlProps = urlPropMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("&"));
        assertSslConfig(props, JdbcConfiguration.create("jdbc:es://test?" + sslUrlProps.toString(), props, 0).sslConfig());
    }

    @SuppressForbidden(reason = "JDBC drivers allows logging to Sys.out")
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/41557")
    public void testDriverConfigurationWithSSLInURL() {
        Map<String, String> urlPropMap = sslProperties();
        String sslUrlProps = urlPropMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("&"));

        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            DriverManager.setLogWriter(new java.io.PrintWriter(System.out));
            return null;
        });

        try {
            DriverManager.getDriver(jdbcPrefix() + "test?" + sslUrlProps);
        } catch (SQLException sqle) {
            fail("Driver registration should have been successful. Error: " + sqle);
        }
    }

    public void testTyposInSslConfigInUrl(){
        assertJdbcSqlExceptionFromUrl("ssl.protocl", "ssl.protocol");
        assertJdbcSqlExceptionFromUrl("sssl", "ssl");
        assertJdbcSqlExceptionFromUrl("ssl.keystore.lction", "ssl.keystore.location");
        assertJdbcSqlExceptionFromUrl("ssl.keystore.pss", "ssl.keystore.pass");
        assertJdbcSqlExceptionFromUrl("ssl.keystore.typ", "ssl.keystore.type");
        assertJdbcSqlExceptionFromUrl("ssl.trustsore.location", "ssl.truststore.location");
        assertJdbcSqlExceptionFromUrl("ssl.tuststore.pass", "ssl.truststore.pass");
        assertJdbcSqlExceptionFromUrl("ssl.ruststore.type", "ssl.truststore.type");
    }

    public void testTyposInSslConfigInProperties() {
        assertJdbcSqlExceptionFromProperties("ssl.protocl", "ssl.protocol");
        assertJdbcSqlExceptionFromProperties("sssl", "ssl");
        assertJdbcSqlExceptionFromProperties("ssl.keystore.lction", "ssl.keystore.location");
        assertJdbcSqlExceptionFromProperties("ssl.keystore.pss", "ssl.keystore.pass");
        assertJdbcSqlExceptionFromProperties("ssl.keystore.typ", "ssl.keystore.type");
        assertJdbcSqlExceptionFromProperties("ssl.trustsore.location", "ssl.truststore.location");
        assertJdbcSqlExceptionFromProperties("ssl.tuststore.pass", "ssl.truststore.pass");
        assertJdbcSqlExceptionFromProperties("ssl.ruststore.type", "ssl.truststore.type");
    }

    static Map<String, String> sslProperties() {
        Map<String, String> sslPropertiesMap = new HashMap<>(8);
        // always using "false" so that the SSLContext doesn't actually start verifying the keystore and trustore
        // locations, as we don't have file permissions to access them.
        sslPropertiesMap.put("ssl", "false");
        sslPropertiesMap.put("ssl.protocol", "SSLv3");
        sslPropertiesMap.put("ssl.keystore.location", "/abc/xyz");
        sslPropertiesMap.put("ssl.keystore.pass", "mypass");
        sslPropertiesMap.put("ssl.keystore.type", "PKCS12");
        sslPropertiesMap.put("ssl.truststore.location", "/foo/bar");
        sslPropertiesMap.put("ssl.truststore.pass", "anotherpass");
        sslPropertiesMap.put("ssl.truststore.type", "jks");

        return sslPropertiesMap;
    }

    static void assertSslConfig(Properties allProperties, SslConfig sslConfig) throws URISyntaxException {
        // because SslConfig doesn't expose its internal properties (and it shouldn't),
        // we compare a newly created SslConfig with the one from the JdbcConfiguration with the equals() method
        SslConfig mockSslConfig = new SslConfig(allProperties, new URI("http://test:9200/"));
        assertEquals(mockSslConfig, sslConfig);
    }

    private void assertJdbcSqlExceptionFromUrl(String wrongSetting, String correctSetting) {
        String url = jdbcPrefix() + "test?" + wrongSetting + "=foo";
        assertJdbcSqlException(wrongSetting, correctSetting, url, null);
    }

    private void assertJdbcSqlExceptionFromProperties(String wrongSetting, String correctSetting) {
        String url = jdbcPrefix() + "test";
        Properties props = new Properties();
        props.put(wrongSetting, correctSetting);
        assertJdbcSqlException(wrongSetting, correctSetting, url, props);
    }

    private void assertJdbcSqlException(String wrongSetting, String correctSetting, String url, Properties props) {
        JdbcSQLException ex = expectThrows(JdbcSQLException.class,
                () -> JdbcConfiguration.create(url, props, 0));
        assertEquals("Unknown parameter [" + wrongSetting + "]; did you mean [" + correctSetting + "]", ex.getMessage());
    }
}
