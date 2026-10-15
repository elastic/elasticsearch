/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class WaitForHttpResourceTests {

    /**
     * The endpoint probed by {@link WaitForHttpResource} is always a loopback address chosen by the test-cluster
     * harness (e.g. "127.0.0.1" or "[::1]"), never a hostname that the cluster's configured certificate is expected
     * to assert. Some JSSE providers (e.g. BouncyCastle's, used under FIPS and observed on some CI images) enforce
     * endpoint identity matching more strictly than SunJSSE and fail with "No hostname specified for HTTPS endpoint
     * ID check" -> certificate_unknown(46) when no verifier is configured. Once SSL is configured, the connection
     * must therefore accept any hostname/session, regardless of what is presented.
     */
    @Test
    public void testConfiguresPermissiveHostnameVerifierWhenSslConfigured() throws Exception {
        WaitForHttpResource wait = new WaitForHttpResource(new URL("https://127.0.0.1:1/"));
        SSLContext ssl = SSLContext.getDefault();

        HttpURLConnection connection = wait.buildConnection(ssl);

        assertThat(connection, instanceOf(HttpsURLConnection.class));
        HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
        assertThat(httpsConnection.getHostnameVerifier().verify("does-not-match-anything", null), is(true));
    }

    /**
     * When no SSL context is configured (i.e. the cluster under test isn't using TLS), the connection is left with
     * the JDK's default hostname verification behaviour rather than being made permissive.
     */
    @Test
    public void testDoesNotRelaxHostnameVerificationWhenSslNotConfigured() throws Exception {
        WaitForHttpResource wait = new WaitForHttpResource(new URL("https://127.0.0.1:1/"));

        HttpURLConnection connection = wait.buildConnection(null);

        HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
        assertThat(httpsConnection.getHostnameVerifier(), is(HttpsURLConnection.getDefaultHostnameVerifier()));
    }
}
