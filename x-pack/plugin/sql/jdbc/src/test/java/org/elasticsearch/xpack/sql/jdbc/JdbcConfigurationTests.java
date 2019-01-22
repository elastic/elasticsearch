/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.sql.SQLException;
import java.util.Properties;

import static org.elasticsearch.xpack.sql.client.ConnectionConfiguration.CONNECT_TIMEOUT;
import static org.elasticsearch.xpack.sql.client.ConnectionConfiguration.PAGE_TIMEOUT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class JdbcConfigurationTests extends ESTestCase {

    private JdbcConfiguration ci(String url) throws SQLException {
        return JdbcConfiguration.create(url, null, 0);
    }

    public void testJustThePrefix() throws Exception {
       Exception e = expectThrows(JdbcSQLException.class, () -> ci("jdbc:es:"));
       assertEquals("Expected [jdbc:es://] url, received [jdbc:es:]", e.getMessage());
    }

    public void testJustTheHost() throws Exception {
        assertThat(ci("jdbc:es://localhost").baseUri().toString(), is("http://localhost:9200/"));
    }

    public void testHostAndPort() throws Exception {
        assertThat(ci("jdbc:es://localhost:1234").baseUri().toString(), is("http://localhost:1234/"));
    }

    public void testTrailingSlashForHost() throws Exception {
        assertThat(ci("jdbc:es://localhost:1234/").baseUri().toString(), is("http://localhost:1234/"));
    }

    public void testMultiPathSuffix() throws Exception {
        assertThat(ci("jdbc:es://a:1/foo/bar/tar").baseUri().toString(), is("http://a:1/foo/bar/tar"));
    }

    public void testV6Localhost() throws Exception {
        assertThat(ci("jdbc:es://[::1]:54161/foo/bar").baseUri().toString(), is("http://[::1]:54161/foo/bar"));
    }

    public void testDebug() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://a:1/?debug=true");
        assertThat(ci.baseUri().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("err"));
    }

    public void testDebugOut() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://a:1/?debug=true&debug.output=jdbc.out");
        assertThat(ci.baseUri().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("jdbc.out"));
    }

    public void testTypeInParam() throws Exception {
        Exception e = expectThrows(JdbcSQLException.class, () -> ci("jdbc:es://a:1/foo/bar/tar?debug=true&debug.out=jdbc.out"));
        assertEquals("Unknown parameter [debug.out] ; did you mean [debug.output]", e.getMessage());
    }

    public void testDebugOutWithSuffix() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://a:1/foo/bar/tar?debug=true&debug.output=jdbc.out");
        assertThat(ci.baseUri().toString(), is("http://a:1/foo/bar/tar"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("jdbc.out"));
    }

    public void testHttpWithSSLEnabledFromProperty() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://test?ssl=true");
        assertThat(ci.baseUri().toString(), is("https://test:9200/"));
    }
    
    public void testHttpWithSSLEnabledFromPropertyAndDisabledFromProtocol() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://http://test?ssl=true");
        assertThat(ci.baseUri().toString(), is("https://test:9200/"));
    }
    
    public void testHttpWithSSLEnabledFromProtocol() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://https://test:9200");
        assertThat(ci.baseUri().toString(), is("https://test:9200/"));
    }
    
    public void testHttpWithSSLEnabledFromProtocolAndProperty() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://https://test:9200?ssl=true");
        assertThat(ci.baseUri().toString(), is("https://test:9200/"));
    }

    public void testHttpWithSSLDisabledFromProperty() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://test?ssl=false");
        assertThat(ci.baseUri().toString(), is("http://test:9200/"));
    }
    
    public void testHttpWithSSLDisabledFromPropertyAndProtocol() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://http://test?ssl=false");
        assertThat(ci.baseUri().toString(), is("http://test:9200/"));
    }
    
    public void testHttpWithSSLDisabledFromPropertyAndEnabledFromProtocol() throws Exception {
        Exception e = expectThrows(JdbcSQLException.class, () -> ci("jdbc:es://https://test?ssl=false"));
        assertEquals("Cannot enable SSL: HTTPS protocol being used in the URL and SSL disabled in properties", e.getMessage());
    }

    public void testTimoutOverride() throws Exception {
        Properties properties  = new Properties();
        properties.setProperty(CONNECT_TIMEOUT, "3"); // Should be overridden
        properties.setProperty(PAGE_TIMEOUT, "4");

        String url = "jdbc:es://test?connect.timeout=1&page.timeout=2";

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


}
