/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl.netty;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.Transport;

/**
 *
 */
public class NettySSLTransportModule extends AbstractModule {

    private final Settings settings;

    public NettySSLTransportModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(NettySSLTransport.class).asEagerSingleton();
        bind(Transport.class).to(NettySSLTransport.class);
    }
}
