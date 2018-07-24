/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;

/**
 * Tests {@link SecurityHttpClientConfigCallback}.
 */
public class SecurityHttpClientConfigCallbackTests extends ESTestCase {

    private final CredentialsProvider credentialsProvider = mock(CredentialsProvider.class);
    private final SSLIOSessionStrategy sslStrategy = mock(SSLIOSessionStrategy.class);
    /**
     * HttpAsyncClientBuilder's methods are {@code final} and therefore not verifiable.
     */
    private final HttpAsyncClientBuilder builder = mock(HttpAsyncClientBuilder.class);

    public void testSSLIOSessionStrategyNullThrowsException() {
        final CredentialsProvider optionalCredentialsProvider = randomFrom(credentialsProvider, null);

        expectThrows(NullPointerException.class, () -> new SecurityHttpClientConfigCallback(null, optionalCredentialsProvider));
    }

    public void testCustomizeHttpClient() {
        final SecurityHttpClientConfigCallback callback = new SecurityHttpClientConfigCallback(sslStrategy, credentialsProvider);

        assertSame(credentialsProvider, callback.getCredentialsProvider());
        assertSame(sslStrategy, callback.getSSLStrategy());

        assertSame(builder, callback.customizeHttpClient(builder));
    }

    public void testCustomizeHttpClientWithOptionalParameters() {
        final CredentialsProvider optionalCredentialsProvider = randomFrom(credentialsProvider, null);

        final SecurityHttpClientConfigCallback callback =
            new SecurityHttpClientConfigCallback(sslStrategy, optionalCredentialsProvider);

        assertSame(builder, callback.customizeHttpClient(builder));
        assertSame(optionalCredentialsProvider, callback.getCredentialsProvider());
        assertSame(sslStrategy, callback.getSSLStrategy());
    }

}
