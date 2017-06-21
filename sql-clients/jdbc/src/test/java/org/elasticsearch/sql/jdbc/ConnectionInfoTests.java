/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.sql.jdbc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcException;

import static org.hamcrest.Matchers.is;

public class ConnectionInfoTests extends ESTestCase {

    private JdbcConfiguration ci(String url) {
        return new JdbcConfiguration(url, null);
    }

    public void testJustThePrefix() throws Exception {
       Exception e = expectThrows(JdbcException.class, () -> ci("jdbc:es:"));
       assertEquals("Invalid URL jdbc:es:, format should be jdbc:es://[host[:port]]*/[prefix]*[?[option=value]&]*", e.getMessage());
    }

    public void testJustTheHost() throws Exception {
        assertThat(ci("jdbc:es://localhost").asUrl().toString(), is("http://localhost:9200/"));
    }

    public void testHostAndPort() throws Exception {
        assertThat(ci("jdbc:es://localhost:1234").asUrl().toString(), is("http://localhost:1234/"));
    }

    public void testTrailingSlashForHost() throws Exception {
        assertThat(ci("jdbc:es://localhost:1234/").asUrl().toString(), is("http://localhost:1234/"));
    }

    public void testMultiPathSuffix() throws Exception {
        assertThat(ci("jdbc:es://a:1/foo/bar/tar").asUrl().toString(), is("http://a:1/foo/bar/tar"));
    }

    public void testDebug() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://a:1/?debug=true");
        assertThat(ci.asUrl().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("err"));
    }

    public void testDebugOut() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://a:1/?debug=true&debug.output=jdbc.out");
        assertThat(ci.asUrl().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("jdbc.out"));
    }

    public void testTypeInParam() throws Exception {
        Exception e = expectThrows(JdbcException.class, () -> ci("jdbc:es://a:1/foo/bar/tar?debug=true&debug.out=jdbc.out"));
        assertEquals("Unknown parameter [debug.out] ; did you mean [debug.output]", e.getMessage());
    }

    public void testDebugOutWithSuffix() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://a:1/foo/bar/tar?debug=true&debug.output=jdbc.out");
        assertThat(ci.asUrl().toString(), is("http://a:1/foo/bar/tar"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("jdbc.out"));
    }
}
