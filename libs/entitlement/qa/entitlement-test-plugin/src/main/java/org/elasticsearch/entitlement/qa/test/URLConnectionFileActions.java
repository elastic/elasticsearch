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
import java.net.JarURLConnection;
import java.net.URLConnection;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

class URLConnectionFileActions {

    private static void withJdkFileConnection(CheckedConsumer<URLConnection, Exception> connectionConsumer) throws Exception {
        var conn = EntitledActions.createFileURLConnection();
        // Be sure we got the connection implementation we want
        assert conn.getClass().getSimpleName().equals("FileURLConnection");
        try {
            connectionConsumer.accept(conn);
        } catch (IOException e) {
            // It's OK, it means we passed entitlement checks, and we tried to perform some operation
        }
    }

    private static void withJarConnection(CheckedConsumer<JarURLConnection, Exception> connectionConsumer) throws Exception {
        var conn = EntitledActions.createJarURLConnection();
        // Be sure we got the connection implementation we want
        assert JarURLConnection.class.isAssignableFrom(conn.getClass());
        connectionConsumer.accept((JarURLConnection) conn);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionConnect() throws Exception {
        withJdkFileConnection(URLConnection::connect);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetHeaderFields() throws Exception {
        withJdkFileConnection(URLConnection::getHeaderFields);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetHeaderFieldWithName() throws Exception {
        withJdkFileConnection(urlConnection -> urlConnection.getHeaderField("date"));
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetHeaderFieldWithIndex() throws Exception {
        withJdkFileConnection(urlConnection -> urlConnection.getHeaderField(0));
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetContentLength() throws Exception {
        withJdkFileConnection(URLConnection::getContentLength);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetContentLengthLong() throws Exception {
        withJdkFileConnection(URLConnection::getContentLengthLong);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetHeaderFieldKey() throws Exception {
        withJdkFileConnection(urlConnection -> urlConnection.getHeaderFieldKey(0));
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetLastModified() throws Exception {
        withJdkFileConnection(URLConnection::getLastModified);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetInputStream() throws Exception {
        withJdkFileConnection(URLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetContentType() throws Exception {
        withJdkFileConnection(URLConnection::getContentType);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetContentEncoding() throws Exception {
        withJdkFileConnection(URLConnection::getContentEncoding);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetExpiration() throws Exception {
        withJdkFileConnection(URLConnection::getExpiration);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetDate() throws Exception {
        withJdkFileConnection(URLConnection::getDate);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetHeaderFieldInt() throws Exception {
        withJdkFileConnection(conn -> conn.getHeaderFieldInt("field", 0));
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetHeaderFieldLong() throws Exception {
        withJdkFileConnection(conn -> conn.getHeaderFieldLong("field", 0));
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetContent() throws Exception {
        withJdkFileConnection(URLConnection::getContent);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String sunFileURLConnectionGetContentWithClasses() throws Exception {
        withJdkFileConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
        return "true";
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

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String netJarURLConnectionGetContentEncoding() throws Exception {
        withJarConnection(URLConnection::getContentEncoding);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String netJarURLConnectionGetExpiration() throws Exception {
        withJarConnection(URLConnection::getExpiration);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String netJarURLConnectionGetDate() throws Exception {
        withJarConnection(URLConnection::getDate);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String netJarURLConnectionGetLastModified() throws Exception {
        withJarConnection(URLConnection::getLastModified);
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String netJarURLConnectionGetHeaderFieldInt() throws Exception {
        withJarConnection(conn -> conn.getHeaderFieldInt("field", 0));
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String netJarURLConnectionGetHeaderFieldLong() throws Exception {
        withJarConnection(conn -> conn.getHeaderFieldLong("field", 0));
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "true")
    static String netJarURLConnectionGetHeaderFieldDate() throws Exception {
        withJarConnection(conn -> conn.getHeaderFieldDate("field", 0));
        return "true";
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void netJarURLConnectionGetContent() throws Exception {
        withJarConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }
}
