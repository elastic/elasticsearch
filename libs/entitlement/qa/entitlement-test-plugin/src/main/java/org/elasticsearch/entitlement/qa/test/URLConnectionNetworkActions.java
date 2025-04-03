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
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.qa.entitled.EntitledActions;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import javax.net.ssl.HttpsURLConnection;

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

    private static void withPlainNetworkConnection(CheckedConsumer<HttpURLConnection, Exception> connectionConsumer) throws Exception {
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

    private static void withJdkHttpsConnection(CheckedConsumer<HttpsURLConnection, Exception> connectionConsumer) throws Exception {
        var conn = EntitledActions.createHttpsURLConnection();
        // Be sure we got the connection implementation we want
        assert HttpsURLConnection.class.isAssignableFrom(conn.getClass());
        try {
            connectionConsumer.accept((HttpsURLConnection) conn);
        } catch (java.net.ConnectException e) {
            // It's OK, it means we passed entitlement checks, and we tried to connect
        }
    }

    private static void withJdkFtpConnection(CheckedConsumer<URLConnection, Exception> connectionConsumer) throws Exception {
        var conn = EntitledActions.createFtpURLConnection();
        // Be sure we got the connection implementation we want
        assert conn.getClass().getSimpleName().equals("FtpURLConnection");
        try {
            connectionConsumer.accept(conn);
        } catch (java.net.ConnectException e) {
            // It's OK, it means we passed entitlement checks, and we tried to connect
        }
    }

    private static void withJdkMailToConnection(CheckedConsumer<URLConnection, Exception> connectionConsumer) throws Exception {
        var conn = EntitledActions.createMailToURLConnection();
        // Be sure we got the connection implementation we want
        assert conn.getClass().getSimpleName().equals("MailToURLConnection");
        try {
            connectionConsumer.accept(conn);
        } catch (IOException e) {
            // It's OK, it means we passed entitlement checks, and we tried to perform some IO
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlOpenConnection() throws Exception {
        URI.create("http://127.0.0.1:12345/").toURL().openConnection();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    @SuppressForbidden(reason = "just testing, not a real connection")
    static void urlOpenConnectionWithProxy() throws URISyntaxException, IOException {
        var url = new URI("http://localhost").toURL();
        var urlConnection = url.openConnection(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(0)));
        assert urlConnection != null;
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlOpenStream() throws Exception {
        try {
            URI.create("http://127.0.0.1:12345/").toURL().openStream().close();
        } catch (java.net.ConnectException e) {
            // It's OK, it means we passed entitlement checks, and we tried to connect
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlGetContent() throws Exception {
        try {
            URI.create("http://127.0.0.1:12345/").toURL().getContent();
        } catch (java.net.ConnectException e) {
            // It's OK, it means we passed entitlement checks, and we tried to connect
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void urlGetContentWithClasses() throws Exception {
        try {
            URI.create("http://127.0.0.1:12345/").toURL().getContent(new Class<?>[] { String.class });
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

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFtpURLConnectionConnect() throws Exception {
        withJdkFtpConnection(URLConnection::connect);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFtpURLConnectionGetInputStream() throws Exception {
        withJdkFtpConnection(URLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFtpURLConnectionGetOutputStream() throws Exception {
        withJdkFtpConnection(URLConnection::getOutputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseHttpURLConnectionGetResponseCode() throws Exception {
        withPlainNetworkConnection(HttpURLConnection::getResponseCode);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseHttpURLConnectionGetResponseMessage() throws Exception {
        withPlainNetworkConnection(HttpURLConnection::getResponseMessage);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void baseHttpURLConnectionGetHeaderFieldDate() throws Exception {
        withPlainNetworkConnection(conn -> conn.getHeaderFieldDate("date", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpURLConnectionConnect() throws Exception {
        withJdkHttpConnection(HttpURLConnection::connect);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpURLConnectionGetOutputStream() throws Exception {
        withJdkHttpConnection(httpURLConnection -> {
            httpURLConnection.setDoOutput(true);
            httpURLConnection.getOutputStream();
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpURLConnectionGetInputStream() throws Exception {
        withJdkHttpConnection(HttpURLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpURLConnectionGetErrorStream() throws Exception {
        withJdkHttpConnection(HttpURLConnection::getErrorStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpURLConnectionGetHeaderFieldWithName() throws Exception {
        withJdkHttpConnection(conn -> conn.getHeaderField("date"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpURLConnectionGetHeaderFields() throws Exception {
        withJdkHttpConnection(HttpURLConnection::getHeaderFields);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpURLConnectionGetHeaderFieldWithIndex() throws Exception {
        withJdkHttpConnection(conn -> conn.getHeaderField(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpURLConnectionGetHeaderFieldKey() throws Exception {
        withJdkHttpConnection(conn -> conn.getHeaderFieldKey(0));
    }

    // https
    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplConnect() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::connect);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetOutputStream() throws Exception {
        withJdkHttpsConnection(httpsURLConnection -> {
            httpsURLConnection.setDoOutput(true);
            httpsURLConnection.getOutputStream();
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetInputStream() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetErrorStream() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getErrorStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetHeaderFieldWithName() throws Exception {
        withJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderField("date"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetHeaderFields() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getHeaderFields);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetHeaderFieldWithIndex() throws Exception {
        withJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderField(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetHeaderFieldKey() throws Exception {
        withJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderFieldKey(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetResponseCode() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getResponseCode);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetResponseMessage() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getResponseMessage);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetContentLength() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImpl$getContentLengthLong() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getContentLengthLong);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetContentType() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetContentEncoding() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetExpiration() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetDate() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetLastModified() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetHeaderFieldInt() throws Exception {
        withJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderFieldInt("content-length", -1));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetHeaderFieldLong() throws Exception {
        withJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderFieldLong("content-length", -1));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetHeaderFieldDate() throws Exception {
        withJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderFieldDate("date", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetContent() throws Exception {
        withJdkHttpsConnection(HttpsURLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunHttpsURLConnectionImplGetContentWithClasses() throws Exception {
        withJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getContent(new Class<?>[] { String.class }));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunMailToURLConnectionConnect() throws Exception {
        withJdkMailToConnection(URLConnection::connect);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunMailToURLConnectionGetOutputStream() throws Exception {
        withJdkMailToConnection(URLConnection::getOutputStream);
    }
}
