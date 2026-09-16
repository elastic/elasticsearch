/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.core.Nullable;

import java.util.Objects;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

/**
 * {@code SecurityHttpClientConfigCallback} configures a {@link RestClient} for user authentication and SSL / TLS.
 */
class SecurityHttpClientConfigCallback implements RestClientBuilder.HttpClientConfigCallback {

    /**
     * The optional {@link CredentialsProvider} for all requests to enable user authentication.
     */
    @Nullable
    private final CredentialsProvider credentialsProvider;
    /**
     * The {@link SSLContext} for all requests to enable SSL / TLS encryption.
     */
    private final SSLContext sslContext;
    /**
     * The {@link HostnameVerifier} for all requests to enable SSL / TLS hostname verification.
     */
    private final HostnameVerifier hostnameVerifier;

    /**
     * Create a new {@link SecurityHttpClientConfigCallback}.
     *
     * @param credentialsProvider The credential provider, if a username/password have been supplied
     * @param sslContext The SSL context for SSL / TLS encryption
     * @param hostnameVerifier The hostname verifier for SSL / TLS
     * @throws NullPointerException if {@code sslContext} is {@code null}
     */
    SecurityHttpClientConfigCallback(
        final SSLContext sslContext,
        final HostnameVerifier hostnameVerifier,
        @Nullable final CredentialsProvider credentialsProvider
    ) {
        this.sslContext = Objects.requireNonNull(sslContext);
        this.hostnameVerifier = Objects.requireNonNull(hostnameVerifier);
        this.credentialsProvider = credentialsProvider;
    }

    /**
     * Get the {@link CredentialsProvider} that will be added to the HTTP client.
     *
     * @return Can be {@code null}.
     */
    @Nullable
    CredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    /**
     * Get the {@link SSLContext} that will be added to the HTTP client.
     *
     * @return Never {@code null}.
     */
    SSLContext getSSLContext() {
        return sslContext;
    }

    /**
     * Get the {@link HostnameVerifier} that will be added to the HTTP client.
     *
     * @return Never {@code null}.
     */
    HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    /**
     * Sets the {@linkplain HttpAsyncClientBuilder#setDefaultCredentialsProvider(CredentialsProvider) credential provider},
     * {@linkplain HttpAsyncClientBuilder#setSSLContext(SSLContext) SSL context}, and
     * {@linkplain HttpAsyncClientBuilder#setSSLHostnameVerifier(HostnameVerifier) SSL Hostname Verifier}.
     *
     * @param httpClientBuilder The client to configure.
     * @return Always {@code httpClientBuilder}.
     */
    @Override
    public HttpAsyncClientBuilder customizeHttpClient(final HttpAsyncClientBuilder httpClientBuilder) {
        // enable SSL / TLS
        httpClientBuilder.setSSLContext(sslContext);
        httpClientBuilder.setSSLHostnameVerifier(hostnameVerifier);

        // enable user authentication
        if (credentialsProvider != null) {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }

        return httpClientBuilder;
    }

}
