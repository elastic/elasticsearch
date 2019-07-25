/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.test.ESTestCase;

import java.net.URI;

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
                "Invalid connection configuration [ftp://server:9201/]: Only http and https protocols are supported",
                expectThrows(IllegalArgumentException.class, () -> parseURI("ftp://server:9201/", DEFAULT_URI)).getMessage()
        );
    }

    public void testMalformed() throws Exception {
        assertEquals(
                "Invalid connection configuration []: Expected authority at index 7: http://",
                expectThrows(IllegalArgumentException.class, () -> parseURI("", DEFAULT_URI)).getMessage()
        );
    }

    public void testRemoveQuery() throws Exception {
        assertEquals(URI.create("http://server:9100"),
                removeQuery(URI.create("http://server:9100?query"), "http://server:9100?query", DEFAULT_URI));
    }

    public void testRemoveQueryTrailingSlash() throws Exception {
        assertEquals(URI.create("http://server:9100/"),
                removeQuery(URI.create("http://server:9100/?query"), "http://server:9100/?query", DEFAULT_URI));
    }

    public void testRemoveQueryNoQuery() throws Exception {
        assertEquals(URI.create("http://server:9100"),
                removeQuery(URI.create("http://server:9100"), "http://server:9100", DEFAULT_URI));
    }
    
    public void testAppendEmptySegmentToPath() throws Exception {
        assertEquals(URI.create("http://server:9100"),
                appendSegmentToPath(URI.create("http://server:9100"), ""));
    }
    
    public void testAppendNullSegmentToPath() throws Exception {
        assertEquals(URI.create("http://server:9100"),
                appendSegmentToPath(URI.create("http://server:9100"), null));
    }
    
    public void testAppendSegmentToNullPath() throws Exception {
        assertEquals(
                "URI must not be null",
                expectThrows(IllegalArgumentException.class, () -> appendSegmentToPath(null, "/_sql")).getMessage()
        );
    }
    
    public void testAppendSegmentToEmptyPath() throws Exception {
        assertEquals(URI.create("/_sql"),
                appendSegmentToPath(URI.create(""), "/_sql"));
    }
    
    public void testAppendSlashSegmentToPath() throws Exception {
        assertEquals(URI.create("http://server:9100"),
                appendSegmentToPath(URI.create("http://server:9100"), "/"));
    }
    
    public void testAppendSqlSegmentToPath() throws Exception {
        assertEquals(URI.create("http://server:9100/_sql"),
                appendSegmentToPath(URI.create("http://server:9100"), "/_sql"));
    }
    
    public void testAppendSqlSegmentNoSlashToPath() throws Exception {
        assertEquals(URI.create("http://server:9100/_sql"),
                appendSegmentToPath(URI.create("http://server:9100"), "_sql"));
    }
    
    public void testAppendSegmentToPath() throws Exception {
        assertEquals(URI.create("http://server:9100/es_rest/_sql"),
                appendSegmentToPath(URI.create("http://server:9100/es_rest"), "/_sql"));
    }
    
    public void testAppendSegmentNoSlashToPath() throws Exception {
        assertEquals(URI.create("http://server:9100/es_rest/_sql"),
                appendSegmentToPath(URI.create("http://server:9100/es_rest"), "_sql"));
    }
    
    public void testAppendSegmentTwoSlashesToPath() throws Exception {
        assertEquals(URI.create("https://server:9100/es_rest/_sql"),
                appendSegmentToPath(URI.create("https://server:9100/es_rest/"), "/_sql"));
    }
}
