/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.transport.Transport;

/**
 *
 */
public class NettySecuredTransportModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Transport.class).to(NettySecuredTransport.class).asEagerSingleton();
    }
}
