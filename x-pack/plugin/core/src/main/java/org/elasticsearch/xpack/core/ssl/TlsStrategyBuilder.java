/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.HostnameVerificationPolicy;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.SSLBufferMode;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

class TlsStrategyBuilder extends AbstractSslBuilder<TlsStrategy> {

    public static final TlsStrategyBuilder INSTANCE = new TlsStrategyBuilder();

    @Override
    TlsStrategy build(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier) {
        // CLIENT uses only the verifier selected from verification_mode. The 5-arg constructor
        // defaults to BOTH, which also enables JSSE HTTPS endpoint identification and would
        // ignore certificate/none.
        return new DefaultClientTlsStrategy(
            sslContext,
            protocols,
            ciphers,
            SSLBufferMode.DYNAMIC,
            HostnameVerificationPolicy.CLIENT,
            verifier
        );
    }
}
