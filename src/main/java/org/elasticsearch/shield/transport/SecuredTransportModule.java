/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.shield.SecurityFilter;
import org.elasticsearch.shield.plugin.SecurityPlugin;
import org.elasticsearch.shield.transport.n2n.IPFilteringN2NAuthenticator;
import org.elasticsearch.shield.transport.netty.N2NNettyUpstreamHandler;
import org.elasticsearch.shield.transport.netty.NettySecuredHttpServerTransportModule;
import org.elasticsearch.shield.transport.netty.NettySecuredTransportModule;
import org.elasticsearch.transport.TransportModule;

/**
 *
 */
public class SecuredTransportModule extends AbstractModule implements SpawnModules, PreProcessModule {

    @Override
    public Iterable<? extends Module> spawnModules() {

        //todo we only need to spawn http module if we're not within the transport client context
        return ImmutableList.of(
                new NettySecuredHttpServerTransportModule(),
                new NettySecuredTransportModule());
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof TransportModule) {
            ((TransportModule) module).setTransportService(SecuredTransportService.class, SecurityPlugin.NAME);
        }
    }

    @Override
    protected void configure() {
        bind(TransportFilter.class).to(SecurityFilter.Transport.class).asEagerSingleton();
        bind(IPFilteringN2NAuthenticator.class).asEagerSingleton();
        bind(N2NNettyUpstreamHandler.class).asEagerSingleton();
    }
}
