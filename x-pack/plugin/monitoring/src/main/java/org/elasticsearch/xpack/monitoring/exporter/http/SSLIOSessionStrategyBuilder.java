/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpHost;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOSession;
import org.elasticsearch.xpack.core.ssl.AbstractSslBuilder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

class SSLIOSessionStrategyBuilder extends AbstractSslBuilder<SSLIOSessionStrategy> {

    static final SSLIOSessionStrategyBuilder INSTANCE = new SSLIOSessionStrategyBuilder();

    @Override
    protected SSLIOSessionStrategy build(SSLContext sslContext, String[] protocols, String[] ciphers, HostnameVerifier verifier) {
        return new SSLIOSessionStrategy(sslContext, protocols, ciphers, verifier) {
            @Override
            protected void verifySession(HttpHost host, IOSession iosession, SSLSession session) throws SSLException {
                verifyHostname(verifier, host.getHostName(), session);
            }
        };
    }
}
