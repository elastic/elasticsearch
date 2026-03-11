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
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.entitlement.qa.entitled.EntitledActions;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URLConnection;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

class URLConnectionFileActions {

    private static boolean isCausedByNotEntitledException(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getClass().getName().equals("org.elasticsearch.entitlement.bridge.NotEntitledException")) {
                return true;
            }
        }
        return false;
    }

    private static void withJdkFileConnection(CheckedConsumer<URLConnection, Exception> connectionConsumer) throws Exception {
        var conn = EntitledActions.createFileURLConnection();
        assert conn.getClass().getSimpleName().equals("FileURLConnection");
        try {
            connectionConsumer.accept(conn);
        } catch (IOException e) {
            if (isCausedByNotEntitledException(e)) {
                throw e;
            }
        }
    }

    private static <R> R callJdkFileConnection(CheckedFunction<URLConnection, R, Exception> connectionFunction) throws Exception {
        var conn = EntitledActions.createFileURLConnection();
        assert conn.getClass().getSimpleName().equals("FileURLConnection");
        try {
            return connectionFunction.apply(conn);
        } catch (IOException e) {
            if (isCausedByNotEntitledException(e)) {
                throw e;
            }
            return null;
        }
    }

    private static void withJarConnection(CheckedConsumer<JarURLConnection, Exception> connectionConsumer) throws Exception {
        var conn = EntitledActions.createJarURLConnection();
        assert JarURLConnection.class.isAssignableFrom(conn.getClass());
        connectionConsumer.accept((JarURLConnection) conn);
    }

    private static <R> R callJarConnection(CheckedFunction<JarURLConnection, R, Exception> connectionFunction) throws Exception {
        var conn = EntitledActions.createJarURLConnection();
        assert JarURLConnection.class.isAssignableFrom(conn.getClass());
        return connectionFunction.apply((JarURLConnection) conn);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionConnect() throws Exception {
        withJdkFileConnection(URLConnection::connect);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "{}", expectedDefaultType = java.util.Map.class)
    static java.util.Map<String, java.util.List<String>> sunFileURLConnectionGetHeaderFields() throws Exception {
        return callJdkFileConnection(URLConnection::getHeaderFields);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunFileURLConnectionGetHeaderFieldWithName() throws Exception {
        return callJdkFileConnection(urlConnection -> urlConnection.getHeaderField("date"));
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunFileURLConnectionGetHeaderFieldWithIndex() throws Exception {
        return callJdkFileConnection(urlConnection -> urlConnection.getHeaderField(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "-1", expectedDefaultType = int.class)
    static int sunFileURLConnectionGetContentLength() throws Exception {
        return callJdkFileConnection(URLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "-1", expectedDefaultType = long.class)
    static long sunFileURLConnectionGetContentLengthLong() throws Exception {
        return callJdkFileConnection(URLConnection::getContentLengthLong);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunFileURLConnectionGetHeaderFieldKey() throws Exception {
        return callJdkFileConnection(urlConnection -> urlConnection.getHeaderFieldKey(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunFileURLConnectionGetLastModified() throws Exception {
        return callJdkFileConnection(URLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetInputStream() throws Exception {
        withJdkFileConnection(URLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunFileURLConnectionGetContentType() throws Exception {
        return callJdkFileConnection(URLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunFileURLConnectionGetContentEncoding() throws Exception {
        return callJdkFileConnection(URLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunFileURLConnectionGetExpiration() throws Exception {
        return callJdkFileConnection(URLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunFileURLConnectionGetDate() throws Exception {
        return callJdkFileConnection(URLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = int.class)
    static int sunFileURLConnectionGetHeaderFieldInt() throws Exception {
        return callJdkFileConnection(conn -> conn.getHeaderFieldInt("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunFileURLConnectionGetHeaderFieldLong() throws Exception {
        return callJdkFileConnection(conn -> conn.getHeaderFieldLong("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunFileURLConnectionGetContent() throws Exception {
        withJdkFileConnection(URLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunFileURLConnectionGetContentWithClasses() throws Exception {
        withJdkFileConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetManifest() throws Exception {
        withJarConnection(JarURLConnection::getManifest);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetJarEntry() throws Exception {
        withJarConnection(JarURLConnection::getJarEntry);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetAttributes() throws Exception {
        withJarConnection(JarURLConnection::getAttributes);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetMainAttributes() throws Exception {
        withJarConnection(JarURLConnection::getMainAttributes);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetCertificates() throws Exception {
        withJarConnection(JarURLConnection::getCertificates);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunJarURLConnectionGetJarFile() throws Exception {
        withJarConnection(JarURLConnection::getJarFile);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunJarURLConnectionGetJarEntry() throws Exception {
        withJarConnection(JarURLConnection::getJarEntry);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunJarURLConnectionConnect() throws Exception {
        withJarConnection(JarURLConnection::connect);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunJarURLConnectionGetInputStream() throws Exception {
        withJarConnection(JarURLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunJarURLConnectionGetContentLength() throws Exception {
        withJarConnection(JarURLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunJarURLConnectionGetContentLengthLong() throws Exception {
        withJarConnection(JarURLConnection::getContentLengthLong);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunJarURLConnectionGetContent() throws Exception {
        withJarConnection(JarURLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunJarURLConnectionGetContentType() throws Exception {
        withJarConnection(JarURLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunJarURLConnectionGetHeaderFieldWithName() throws Exception {
        withJarConnection(conn -> conn.getHeaderField("field"));
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String netJarURLConnectionGetContentEncoding() throws Exception {
        return callJarConnection(URLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long netJarURLConnectionGetExpiration() throws Exception {
        return callJarConnection(URLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long netJarURLConnectionGetDate() throws Exception {
        return callJarConnection(URLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long netJarURLConnectionGetLastModified() throws Exception {
        return callJarConnection(URLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = int.class)
    static int netJarURLConnectionGetHeaderFieldInt() throws Exception {
        return callJarConnection(conn -> conn.getHeaderFieldInt("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long netJarURLConnectionGetHeaderFieldLong() throws Exception {
        return callJarConnection(conn -> conn.getHeaderFieldLong("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long netJarURLConnectionGetHeaderFieldDate() throws Exception {
        return callJarConnection(conn -> conn.getHeaderFieldDate("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void netJarURLConnectionGetContent() throws Exception {
        withJarConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }
}
