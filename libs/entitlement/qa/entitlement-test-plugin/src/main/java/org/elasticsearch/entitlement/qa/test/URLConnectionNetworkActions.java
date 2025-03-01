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
import org.elasticsearch.entitlement.qa.entitled.EntitledActions;

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
        var conn = EntitledActions.createHttpURLConnection();
        // Be sure we got the connection implementation we want
        assert HttpURLConnection.class.isAssignableFrom(conn.getClass());
        try {
            connectionConsumer.accept((HttpURLConnection) conn);
        } catch (java.net.ConnectException e) {
            // It's OK, it means we passed entitlement checks, and we tried to connect
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseUrlConnectionGetContentLength() throws Exception {
        withPlainNetworkConnection(URLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpConnectionGetContentLength() throws Exception {
        withJdkHttpConnection(URLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseUrlConnectionGetContentType() throws Exception {
        withPlainNetworkConnection(URLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpConnectionGetContentType() throws Exception {
        withJdkHttpConnection(URLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseUrlConnectionGetContentEncoding() throws Exception {
        withPlainNetworkConnection(URLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpConnectionGetContentEncoding() throws Exception {
        withJdkHttpConnection(URLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseUrlConnectionGetExpiration() throws Exception {
        withPlainNetworkConnection(URLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpConnectionGetExpiration() throws Exception {
        withJdkHttpConnection(URLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseUrlConnectionGetDate() throws Exception {
        withPlainNetworkConnection(URLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpConnectionGetDate() throws Exception {
        withJdkHttpConnection(URLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseUrlConnectionGetLastModified() throws Exception {
        withPlainNetworkConnection(URLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpConnectionGetLastModified() throws Exception {
        withJdkHttpConnection(URLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseUrlConnectionGetHeaderFieldInt() throws Exception {
        withPlainNetworkConnection(conn -> conn.getHeaderFieldInt("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpConnectionGetHeaderFieldInt() throws Exception {
        withJdkHttpConnection(conn -> conn.getHeaderFieldInt("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseUrlConnectionGetHeaderFieldLong() throws Exception {
        withPlainNetworkConnection(conn -> conn.getHeaderFieldLong("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpConnectionGetHeaderFieldLong() throws Exception {
        withJdkHttpConnection(conn -> conn.getHeaderFieldLong("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseUrlConnectionGetContent() throws Exception {
        withPlainNetworkConnection(URLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpConnectionGetContent() throws Exception {
        withJdkHttpConnection(URLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseUrlConnectionGetContentWithClasses() throws Exception {
        withPlainNetworkConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpConnectionGetContentWithClasses() throws Exception {
        withJdkHttpConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }
}
