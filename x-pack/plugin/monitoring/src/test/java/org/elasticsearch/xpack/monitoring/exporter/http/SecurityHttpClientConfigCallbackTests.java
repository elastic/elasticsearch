/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.test.ESTestCase;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import static org.mockito.Mockito.mock;

/**
 * Tests {@link SecurityHttpClientConfigCallback}.
 */
public class SecurityHttpClientConfigCallbackTests extends ESTestCase {

    private final CredentialsProvider credentialsProvider = mock(CredentialsProvider.class);
    private final SSLContext sslContext = mock(SSLContext.class);
    private final HostnameVerifier hostnameVerifier = mock(HostnameVerifier.class);
    /**
     * HttpAsyncClientBuilder's methods are {@code final} and therefore not verifiable.
     */
    private final HttpAsyncClientBuilder builder = mock(HttpAsyncClientBuilder.class);

    public void testSSLContextNullThrowsException() {
        final CredentialsProvider optionalCredentialsProvider = randomFrom(credentialsProvider, null);

        expectThrows(
            NullPointerException.class,
            () -> new SecurityHttpClientConfigCallback(null, hostnameVerifier, optionalCredentialsProvider)
        );
    }

    public void testHostnameVerifierNullThrowsException() {
        final CredentialsProvider optionalCredentialsProvider = randomFrom(credentialsProvider, null);

        expectThrows(NullPointerException.class, () -> new SecurityHttpClientConfigCallback(sslContext, null, optionalCredentialsProvider));
    }

    public void testCustomizeHttpClient() {
        final SecurityHttpClientConfigCallback callback = new SecurityHttpClientConfigCallback(
            sslContext,
            hostnameVerifier,
            credentialsProvider
        );

        assertSame(credentialsProvider, callback.getCredentialsProvider());
        assertSame(sslContext, callback.getSSLContext());
        assertSame(hostnameVerifier, callback.getHostnameVerifier());

        assertSame(builder, callback.customizeHttpClient(builder));
    }

    public void testCustomizeHttpClientWithOptionalParameters() {
        final CredentialsProvider optionalCredentialsProvider = randomFrom(credentialsProvider, null);

        final SecurityHttpClientConfigCallback callback = new SecurityHttpClientConfigCallback(
            sslContext,
            hostnameVerifier,
            optionalCredentialsProvider
        );

        assertSame(builder, callback.customizeHttpClient(builder));
        assertSame(optionalCredentialsProvider, callback.getCredentialsProvider());
        assertSame(sslContext, callback.getSSLContext());
        assertSame(hostnameVerifier, callback.getHostnameVerifier());
    }

}
