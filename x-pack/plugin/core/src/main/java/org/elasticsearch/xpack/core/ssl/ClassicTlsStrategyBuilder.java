/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

class ClassicTlsStrategyBuilder extends AbstractSslBuilder<SSLConnectionSocketFactory> {

    public static final ClassicTlsStrategyBuilder INSTANCE = new ClassicTlsStrategyBuilder();

    @Override
    SSLConnectionSocketFactory build(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier) {
        return new SSLConnectionSocketFactory(sslContext, protocols, ciphers, verifier);
    }
}
