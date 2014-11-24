/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.support.AbstractShieldModule;
import org.elasticsearch.shield.transport.n2n.IPFilteringN2NAuthenticator;
import org.elasticsearch.shield.transport.netty.NettySecuredHttpServerTransportModule;
import org.elasticsearch.shield.transport.netty.NettySecuredTransportModule;
import org.elasticsearch.transport.TransportModule;

/**
 *
 */
public class SecuredTransportModule extends AbstractShieldModule.Spawn implements PreProcessModule {

    public SecuredTransportModule(Settings settings) {
        super(settings);
    }

    @Override
    public Iterable<? extends Module> spawnModules(boolean clientMode) {

        if (clientMode) {
            return ImmutableList.of(new NettySecuredTransportModule(settings));
        }

        return ImmutableList.of(
                new NettySecuredHttpServerTransportModule(settings),
                new NettySecuredTransportModule(settings));
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof TransportModule) {
            ((TransportModule) module).setTransportService(SecuredTransportService.class, ShieldPlugin.NAME);
        }
    }

    @Override
    protected void configure(boolean clientMode) {

        if (clientMode) {
            // no ip filtering on the client
            bind(IPFilteringN2NAuthenticator.class).toProvider(Providers.<IPFilteringN2NAuthenticator>of(null));
            bind(ServerTransportFilter.class).to(ServerTransportFilter.Client.class).asEagerSingleton();
            bind(ClientTransportFilter.class).to(ClientTransportFilter.Client.class).asEagerSingleton();
            return;
        }

        bind(ServerTransportFilter.class).to(ServerTransportFilter.Node.class).asEagerSingleton();
        bind(ClientTransportFilter.class).to(ClientTransportFilter.Node.class).asEagerSingleton();
        if (settings.getAsBoolean("shield.transport.filter.enabled", true)) {
            bind(IPFilteringN2NAuthenticator.class).asEagerSingleton();
        }
    }
}
