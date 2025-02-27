/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressWarnings("unused") // everything is called via reflection
class URLConnectionNetworkActions {

    private static final URL HTTP_URL;

    static {
        try {
            HTTP_URL = URI.create("http://127.0.0.1/").toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void withPlainNetworkConnection(CheckedConsumer<URLConnection, Exception> connectionConsumer) throws Exception {
        // Create a HttpURLConnection with minimal overrides to test calling directly into URLConnection methods as much as possible
        var conn = new HttpURLConnection(HTTP_URL) {
            @Override
            public void connect() {}

            @Override
            public void disconnect() {}

            @Override
            public boolean usingProxy() {
                return false;
            }

            @Override
            public InputStream getInputStream() throws IOException {
                // Mock an attempt to call connect
                throw new ConnectException();
            }
        };

        try {
            connectionConsumer.accept(conn);
        } catch (java.net.ConnectException e) {
            // It's OK, it means we passed entitlement checks, and we tried to connect
        }
    }

    private static void withJdkHttpConnection(CheckedConsumer<HttpURLConnection, Exception> connectionConsumer) throws Exception {
        var conn = URI.create("http://127.0.0.1:12345/").toURL().openConnection();
        // Be sure we got the connection implementation we want
        assert HttpURLConnection.class.isAssignableFrom(conn.getClass());
        try {
            connectionConsumer.accept((HttpURLConnection) conn);
        } catch (java.net.ConnectException e) {
            // It's OK, it means we passed entitlement checks, and we tried to connect
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlConnectionGetContentLength() throws Exception {
        withPlainNetworkConnection(URLConnection::getContentLength);
        withJdkHttpConnection(URLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlConnectionGetContentType() throws Exception {
        withPlainNetworkConnection(URLConnection::getContentType);
        withJdkHttpConnection(URLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlConnectionGetContentEncoding() throws Exception {
        withPlainNetworkConnection(URLConnection::getContentEncoding);
        withJdkHttpConnection(URLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlConnectionGetExpiration() throws Exception {
        withPlainNetworkConnection(URLConnection::getExpiration);
        withJdkHttpConnection(URLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlConnectionGetDate() throws Exception {
        withPlainNetworkConnection(URLConnection::getDate);
        withJdkHttpConnection(URLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlConnectionGetLastModified() throws Exception {
        withPlainNetworkConnection(URLConnection::getLastModified);
        withJdkHttpConnection(URLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlConnectionGetHeaderFieldInt() throws Exception {
        withPlainNetworkConnection(conn -> conn.getHeaderFieldInt("field", 0));
        withJdkHttpConnection(conn -> conn.getHeaderFieldInt("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlConnectionGetHeaderFieldLong() throws Exception {
        withPlainNetworkConnection(conn -> conn.getHeaderFieldLong("field", 0));
        withJdkHttpConnection(conn -> conn.getHeaderFieldLong("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlConnectionGetContent() throws Exception {
        withPlainNetworkConnection(URLConnection::getContent);
        withJdkHttpConnection(URLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlConnectionGetContentWithClasses() throws Exception {
        withPlainNetworkConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
        withJdkHttpConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }
}
