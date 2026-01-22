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

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetHeaderFields() throws Exception {
        withJdkFileConnection(URLConnection::getHeaderFields);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetHeaderFieldWithName() throws Exception {
        withJdkFileConnection(urlConnection -> urlConnection.getHeaderField("date"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetHeaderFieldWithIndex() throws Exception {
        withJdkFileConnection(urlConnection -> urlConnection.getHeaderField(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetContentLength() throws Exception {
        withJdkFileConnection(URLConnection::getContentLength);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetContentLengthLong() throws Exception {
        withJdkFileConnection(URLConnection::getContentLengthLong);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetHeaderFieldKey() throws Exception {
        withJdkFileConnection(urlConnection -> urlConnection.getHeaderFieldKey(0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetLastModified() throws Exception {
        withJdkFileConnection(URLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetInputStream() throws Exception {
        withJdkFileConnection(URLConnection::getInputStream);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetContentType() throws Exception {
        withJdkFileConnection(URLConnection::getContentType);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetContentEncoding() throws Exception {
        withJdkFileConnection(URLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetExpiration() throws Exception {
        withJdkFileConnection(URLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetDate() throws Exception {
        withJdkFileConnection(URLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetHeaderFieldInt() throws Exception {
        withJdkFileConnection(conn -> conn.getHeaderFieldInt("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetHeaderFieldLong() throws Exception {
        withJdkFileConnection(conn -> conn.getHeaderFieldLong("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sunFileURLConnectionGetContent() throws Exception {
        withJdkFileConnection(URLConnection::getContent);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
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

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetContentEncoding() throws Exception {
        withJarConnection(URLConnection::getContentEncoding);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetExpiration() throws Exception {
        withJarConnection(URLConnection::getExpiration);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetDate() throws Exception {
        withJarConnection(URLConnection::getDate);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetLastModified() throws Exception {
        withJarConnection(URLConnection::getLastModified);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetHeaderFieldInt() throws Exception {
        withJarConnection(conn -> conn.getHeaderFieldInt("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetHeaderFieldLong() throws Exception {
        withJarConnection(conn -> conn.getHeaderFieldLong("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetHeaderFieldDate() throws Exception {
        withJarConnection(conn -> conn.getHeaderFieldDate("field", 0));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void netJarURLConnectionGetContent() throws Exception {
        withJarConnection(conn -> conn.getContent(new Class<?>[] { String.class }));
    }
}
