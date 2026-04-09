/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.CheckedFunction;
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

    private static boolean isCausedByNotEntitledException(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getClass().getName().equals("org.elasticsearch.entitlement.bridge.NotEntitledException")) {
                return true;
            }
        }
        return false;
    }

    private static HttpURLConnection createPlainNetworkConnection() {
        return new HttpURLConnection(HTTP_URL) {
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
                throw new ConnectException();
            }
        };
    }

    private static <R> R callPlainNetworkConnection(CheckedFunction<HttpURLConnection, R, Exception> connectionFunction) throws Exception {
        try {
            return connectionFunction.apply(createPlainNetworkConnection());
        } catch (IOException e) {
            if (isCausedByNotEntitledException(e)) {
                throw e;
            }
            return null;
        }
    }

    private static <R> R callJdkHttpConnection(CheckedFunction<HttpURLConnection, R, Exception> connectionFunction) throws Exception {
        var conn = EntitledActions.createHttpURLConnection();
        // Be sure we got the connection implementation we want
        assert HttpURLConnection.class.isAssignableFrom(conn.getClass());
        try {
            return connectionFunction.apply((HttpURLConnection) conn);
        } catch (IOException e) {
            if (isCausedByNotEntitledException(e)) {
                throw e;
            }
            return null;
        }
    }

    private static <R> R callJdkHttpsConnection(CheckedFunction<HttpsURLConnection, R, Exception> connectionFunction) throws Exception {
        var conn = EntitledActions.createHttpsURLConnection();
        // Be sure we got the connection implementation we want
        assert HttpsURLConnection.class.isAssignableFrom(conn.getClass());
        try {
            return connectionFunction.apply((HttpsURLConnection) conn);
        } catch (IOException e) {
            if (isCausedByNotEntitledException(e)) {
                throw e;
            }
            return null;
        }
    }

    private static <R> R callJdkFtpConnection(CheckedFunction<URLConnection, R, Exception> connectionFunction) throws Exception {
        var conn = EntitledActions.createFtpURLConnection();
        // Be sure we got the connection implementation we want
        assert conn.getClass().getSimpleName().equals("FtpURLConnection");
        try {
            return connectionFunction.apply(conn);
        } catch (IOException e) {
            if (isCausedByNotEntitledException(e)) {
                throw e;
            }
            return null;
        }
    }

    private static <R> R callJdkMailToConnection(CheckedFunction<URLConnection, R, Exception> connectionFunction) throws Exception {
        var conn = EntitledActions.createMailToURLConnection();
        // Be sure we got the connection implementation we want
        assert conn.getClass().getSimpleName().equals("MailToURLConnection");
        try {
            return connectionFunction.apply(conn);
        } catch (IOException e) {
            if (isCausedByNotEntitledException(e)) {
                throw e;
            }
            return null;
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void urlOpenConnection() throws Exception {
        URI.create("http://127.0.0.1:12345/").toURL().openConnection();
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    @SuppressForbidden(reason = "just testing, not a real connection")
    static void urlOpenConnectionWithProxy() throws URISyntaxException, IOException {
        var url = new URI("http://localhost").toURL();
        var urlConnection = url.openConnection(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(0)));
        assert urlConnection != null;
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void urlOpenStream() throws Exception {
        try {
            URI.create("http://127.0.0.1:12345/").toURL().openStream().close();
        } catch (java.net.ConnectException e) {
            // It's OK, it means we passed entitlement checks, and we tried to connect
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void urlGetContent() throws Exception {
        try {
            URI.create("http://127.0.0.1:12345/").toURL().getContent();
        } catch (java.net.ConnectException e) {
            // It's OK, it means we passed entitlement checks, and we tried to connect
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void urlGetContentWithClasses() throws Exception {
        try {
            URI.create("http://127.0.0.1:12345/").toURL().getContent(new Class<?>[] { String.class });
        } catch (java.net.ConnectException e) {
            // It's OK, it means we passed entitlement checks, and we tried to connect
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "-1", expectedDefaultType = int.class)
    static int baseUrlConnectionGetContentLength() throws Exception {
        return callPlainNetworkConnection(URLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "-1", expectedDefaultType = int.class)
    static int sunHttpConnectionGetContentLength() throws Exception {
        return callJdkHttpConnection(URLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String baseUrlConnectionGetContentType() throws Exception {
        return callPlainNetworkConnection(URLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunHttpConnectionGetContentType() throws Exception {
        return callJdkHttpConnection(URLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String baseUrlConnectionGetContentEncoding() throws Exception {
        return callPlainNetworkConnection(URLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunHttpConnectionGetContentEncoding() throws Exception {
        return callJdkHttpConnection(URLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long baseUrlConnectionGetExpiration() throws Exception {
        return callPlainNetworkConnection(URLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunHttpConnectionGetExpiration() throws Exception {
        return callJdkHttpConnection(URLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long baseUrlConnectionGetDate() throws Exception {
        return callPlainNetworkConnection(URLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunHttpConnectionGetDate() throws Exception {
        return callJdkHttpConnection(URLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long baseUrlConnectionGetLastModified() throws Exception {
        return callPlainNetworkConnection(URLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunHttpConnectionGetLastModified() throws Exception {
        return callJdkHttpConnection(URLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = int.class)
    static int baseUrlConnectionGetHeaderFieldInt() throws Exception {
        return callPlainNetworkConnection(conn -> conn.getHeaderFieldInt("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = int.class)
    static int sunHttpConnectionGetHeaderFieldInt() throws Exception {
        return callJdkHttpConnection(conn -> conn.getHeaderFieldInt("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long baseUrlConnectionGetHeaderFieldLong() throws Exception {
        return callPlainNetworkConnection(conn -> conn.getHeaderFieldLong("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunHttpConnectionGetHeaderFieldLong() throws Exception {
        return callJdkHttpConnection(conn -> conn.getHeaderFieldLong("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void baseUrlConnectionGetContent() throws Exception {
        callPlainNetworkConnection(URLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpConnectionGetContent() throws Exception {
        callJdkHttpConnection(URLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void baseUrlConnectionGetContentWithClasses() throws Exception {
        callPlainNetworkConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpConnectionGetContentWithClasses() throws Exception {
        callJdkHttpConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunFtpURLConnectionConnect() throws Exception {
        callJdkFtpConnection(conn -> {
            conn.connect();
            return null;
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunFtpURLConnectionGetInputStream() throws Exception {
        callJdkFtpConnection(URLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunFtpURLConnectionGetOutputStream() throws Exception {
        callJdkFtpConnection(URLConnection::getOutputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void baseHttpURLConnectionGetResponseCode() throws Exception {
        callPlainNetworkConnection(HttpURLConnection::getResponseCode);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void baseHttpURLConnectionGetResponseMessage() throws Exception {
        callPlainNetworkConnection(HttpURLConnection::getResponseMessage);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long baseHttpURLConnectionGetHeaderFieldDate() throws Exception {
        return callPlainNetworkConnection(conn -> conn.getHeaderFieldDate("date", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpURLConnectionConnect() throws Exception {
        callJdkHttpConnection(conn -> {
            conn.connect();
            return null;
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpURLConnectionGetOutputStream() throws Exception {
        callJdkHttpConnection(conn -> {
            conn.setDoOutput(true);
            return conn.getOutputStream();
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpURLConnectionGetInputStream() throws Exception {
        callJdkHttpConnection(HttpURLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static InputStream sunHttpURLConnectionGetErrorStream() throws Exception {
        return callJdkHttpConnection(HttpURLConnection::getErrorStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunHttpURLConnectionGetHeaderFieldWithName() throws Exception {
        return callJdkHttpConnection(conn -> conn.getHeaderField("date"));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "{}", expectedDefaultType = java.util.Map.class)
    static java.util.Map<String, java.util.List<String>> sunHttpURLConnectionGetHeaderFields() throws Exception {
        return callJdkHttpConnection(HttpURLConnection::getHeaderFields);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunHttpURLConnectionGetHeaderFieldWithIndex() throws Exception {
        return callJdkHttpConnection(conn -> conn.getHeaderField(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunHttpURLConnectionGetHeaderFieldKey() throws Exception {
        return callJdkHttpConnection(conn -> conn.getHeaderFieldKey(0));
    }

    // https
    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpsURLConnectionImplConnect() throws Exception {
        callJdkHttpsConnection(conn -> {
            conn.connect();
            return null;
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpsURLConnectionImplGetOutputStream() throws Exception {
        callJdkHttpsConnection(conn -> {
            conn.setDoOutput(true);
            return conn.getOutputStream();
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpsURLConnectionImplGetInputStream() throws Exception {
        callJdkHttpsConnection(HttpsURLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static InputStream sunHttpsURLConnectionImplGetErrorStream() throws Exception {
        return callJdkHttpsConnection(HttpsURLConnection::getErrorStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunHttpsURLConnectionImplGetHeaderFieldWithName() throws Exception {
        return callJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderField("date"));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "{}", expectedDefaultType = java.util.Map.class)
    static java.util.Map<String, java.util.List<String>> sunHttpsURLConnectionImplGetHeaderFields() throws Exception {
        return callJdkHttpsConnection(HttpsURLConnection::getHeaderFields);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunHttpsURLConnectionImplGetHeaderFieldWithIndex() throws Exception {
        return callJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderField(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunHttpsURLConnectionImplGetHeaderFieldKey() throws Exception {
        return callJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderFieldKey(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpsURLConnectionImplGetResponseCode() throws Exception {
        callJdkHttpsConnection(HttpsURLConnection::getResponseCode);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpsURLConnectionImplGetResponseMessage() throws Exception {
        callJdkHttpsConnection(HttpsURLConnection::getResponseMessage);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "-1", expectedDefaultType = int.class)
    static int sunHttpsURLConnectionImplGetContentLength() throws Exception {
        return callJdkHttpsConnection(HttpsURLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "-1", expectedDefaultType = long.class)
    static long sunHttpsURLConnectionImpl$getContentLengthLong() throws Exception {
        return callJdkHttpsConnection(HttpsURLConnection::getContentLengthLong);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunHttpsURLConnectionImplGetContentType() throws Exception {
        return callJdkHttpsConnection(HttpsURLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunHttpsURLConnectionImplGetContentEncoding() throws Exception {
        return callJdkHttpsConnection(HttpsURLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunHttpsURLConnectionImplGetExpiration() throws Exception {
        return callJdkHttpsConnection(HttpsURLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunHttpsURLConnectionImplGetDate() throws Exception {
        return callJdkHttpsConnection(HttpsURLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunHttpsURLConnectionImplGetLastModified() throws Exception {
        return callJdkHttpsConnection(HttpsURLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "-1", expectedDefaultType = int.class)
    static int sunHttpsURLConnectionImplGetHeaderFieldInt() throws Exception {
        return callJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderFieldInt("content-length", -1));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "-1", expectedDefaultType = long.class)
    static long sunHttpsURLConnectionImplGetHeaderFieldLong() throws Exception {
        return callJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderFieldLong("content-length", -1));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunHttpsURLConnectionImplGetHeaderFieldDate() throws Exception {
        return callJdkHttpsConnection(httpsURLConnection -> httpsURLConnection.getHeaderFieldDate("date", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpsURLConnectionImplGetContent() throws Exception {
        callJdkHttpsConnection(HttpsURLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunHttpsURLConnectionImplGetContentWithClasses() throws Exception {
        callJdkHttpsConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunMailToURLConnectionConnect() throws Exception {
        callJdkMailToConnection(conn -> {
            conn.connect();
            return null;
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunMailToURLConnectionGetOutputStream() throws Exception {
        callJdkMailToConnection(URLConnection::getOutputStream);
    }
}
