/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.sql.jdbc;

import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcException;
import org.junit.Test;

import static org.junit.Assert.assertThat;

import static org.hamcrest.Matchers.is;

public class ConnectionInfoTest {

    private JdbcConfiguration ci(String url) {
        return new JdbcConfiguration(url, null);
    }

    @Test(expected = JdbcException.class)
    public void testJustThePrefix() throws Exception {
        ci("jdbc:es:");
    }

    @Test
    public void testJustTheHost() throws Exception {
        assertThat(ci("jdbc:es://localhost").asUrl().toString(), is("http://localhost:9200/"));
    }

    @Test
    public void testHostAndPort() throws Exception {
        assertThat(ci("jdbc:es://localhost:1234").asUrl().toString(), is("http://localhost:1234/"));
    }

    @Test
    public void testTrailingSlashForHost() throws Exception {
        assertThat(ci("jdbc:es://localhost:1234/").asUrl().toString(), is("http://localhost:1234/"));
    }

    @Test
    public void testMultiPathSuffix() throws Exception {
        assertThat(ci("jdbc:es://a:1/foo/bar/tar").asUrl().toString(), is("http://a:1/foo/bar/tar"));
    }

    @Test
    public void testDebug() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://a:1/?debug=true");
        assertThat(ci.asUrl().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("err"));
    }

    @Test
    public void testDebugOut() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://a:1/?debug=true&debug.output=jdbc.out");
        assertThat(ci.asUrl().toString(), is("http://a:1/"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("jdbc.out"));
    }

    @Test(expected = JdbcException.class)
    public void testTypeInParam() throws Exception {
        ci("jdbc:es://a:1/foo/bar/tar?debug=true&debug.out=jdbc.out");
    }

    @Test
    public void testDebugOutWithSuffix() throws Exception {
        JdbcConfiguration ci = ci("jdbc:es://a:1/foo/bar/tar?debug=true&debug.output=jdbc.out");
        assertThat(ci.asUrl().toString(), is("http://a:1/foo/bar/tar"));
        assertThat(ci.debug(), is(true));
        assertThat(ci.debugOut(), is("jdbc.out"));
    }
}
