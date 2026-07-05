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
import org.elasticsearch.entitlement.qa.entitled.EntitledActions;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URLConnection;
import java.util.Map;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

class URLConnectionFileActions {

    private static <R> R callJdkFileConnection(CheckedFunction<URLConnection, R, Exception> connectionFunction) throws Exception {
        var conn = EntitledActions.createFileURLConnection();
        // Be sure we got the connection implementation we want
        assert conn.getClass().getSimpleName().equals("FileURLConnection");
        try {
            return connectionFunction.apply(conn);
        } catch (IOException e) {
            if (e.getCause() != null && e.getCause().getClass().getSimpleName().equals("NotEntitledException")) {
                throw e;
            }
            return null;
        }
    }

    private static <R> R callJarConnection(CheckedFunction<JarURLConnection, R, Exception> connectionFunction) throws Exception {
        var conn = EntitledActions.createJarURLConnection();
        // Be sure we got the connection implementation we want
        assert JarURLConnection.class.isAssignableFrom(conn.getClass());
        return connectionFunction.apply((JarURLConnection) conn);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunFileURLConnectionConnect() throws Exception {
        callJdkFileConnection(conn -> {
            conn.connect();
            return null;
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "{}", expectedDefaultType = Map.class)
    static Map<String, java.util.List<String>> sunFileURLConnectionGetHeaderFields() throws Exception {
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

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunFileURLConnectionGetInputStream() throws Exception {
        callJdkFileConnection(URLConnection::getInputStream);
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
        callJdkFileConnection(URLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunFileURLConnectionGetContentWithClasses() throws Exception {
        callJdkFileConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void netJarURLConnectionGetManifest() throws Exception {
        callJarConnection(JarURLConnection::getManifest);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void netJarURLConnectionGetJarEntry() throws Exception {
        callJarConnection(JarURLConnection::getJarEntry);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void netJarURLConnectionGetAttributes() throws Exception {
        callJarConnection(JarURLConnection::getAttributes);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void netJarURLConnectionGetMainAttributes() throws Exception {
        callJarConnection(JarURLConnection::getMainAttributes);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void netJarURLConnectionGetCertificates() throws Exception {
        callJarConnection(JarURLConnection::getCertificates);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunJarURLConnectionGetJarFile() throws Exception {
        // Use a lambda instead of a method reference to avoid exposing JarFile as a type parameter,
        // which would trigger a forbiddenApis violation.
        callJarConnection(conn -> {
            conn.getJarFile();
            return null;
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunJarURLConnectionGetJarEntry() throws Exception {
        callJarConnection(JarURLConnection::getJarEntry);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunJarURLConnectionConnect() throws Exception {
        callJarConnection(conn -> {
            conn.connect();
            return null;
        });
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunJarURLConnectionGetInputStream() throws Exception {
        callJarConnection(JarURLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "-1", expectedDefaultType = int.class)
    static int sunJarURLConnectionGetContentLength() throws Exception {
        return callJarConnection(JarURLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "-1", expectedDefaultType = long.class)
    static long sunJarURLConnectionGetContentLengthLong() throws Exception {
        return callJarConnection(JarURLConnection::getContentLengthLong);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void sunJarURLConnectionGetContent() throws Exception {
        callJarConnection(JarURLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunJarURLConnectionGetContentType() throws Exception {
        return callJarConnection(JarURLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunJarURLConnectionGetHeaderFieldWithName() throws Exception {
        return callJarConnection(conn -> conn.getHeaderField("field"));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "{}", expectedDefaultType = Map.class)
    static Map<?, ?> sunJarURLConnectionGetHeaderFields() throws Exception {
        return callJarConnection(JarURLConnection::getHeaderFields);
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunJarURLConnectionGetHeaderFieldWithIndex() throws Exception {
        return callJarConnection(conn -> conn.getHeaderField(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, isExpectedDefaultNull = true)
    static String sunJarURLConnectionGetHeaderFieldKey() throws Exception {
        return callJarConnection(conn -> conn.getHeaderFieldKey(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "0", expectedDefaultType = long.class)
    static long sunJarURLConnectionGetLastModified() throws Exception {
        return callJarConnection(JarURLConnection::getLastModified);
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
        callJarConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }
}
