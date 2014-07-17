/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl.netty;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty.NettyHttpServerTransport;

/**
 *
 */
public class NettySSLHttpServerTransportModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(HttpServerTransport.class).to(NettySSLHttpServerTransport.class).asEagerSingleton();
    }

}
