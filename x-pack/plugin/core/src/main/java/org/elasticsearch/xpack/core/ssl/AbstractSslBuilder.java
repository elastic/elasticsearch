/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ssl.SslConfiguration;

import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

public abstract class AbstractSslBuilder<T> {

    public T build(SslConfiguration config, SSLContext sslContext) {
        String[] ciphers = supportedCiphers(sslParameters(sslContext).getCipherSuites(), config.getCipherSuites(), false);
        String[] supportedProtocols = config.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        HostnameVerifier verifier;

        if (config.verificationMode().isHostnameVerificationEnabled()) {
            verifier = SSLIOSessionStrategy.getDefaultHostnameVerifier();
        } else {
            verifier = NoopHostnameVerifier.INSTANCE;
        }

        return build(sslContext, supportedProtocols, ciphers, verifier);
    }

    /**
     * This method exists to simplify testing
     */
    String[] supportedCiphers(String[] supportedCiphers, List<String> requestedCiphers, boolean log) {
        return SSLService.supportedCiphers(supportedCiphers, requestedCiphers, log);
    }

    /**
     * The {@link SSLParameters} that are associated with the {@code sslContext}.
     * <p>
     * This method exists to simplify testing since {@link SSLContext#getSupportedSSLParameters()} is {@code final}.
     *
     * @param sslContext The SSL context for the current SSL settings
     * @return Never {@code null}.
     */
    SSLParameters sslParameters(SSLContext sslContext) {
        return sslContext.getSupportedSSLParameters();
    }

    abstract T build(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier);
}
