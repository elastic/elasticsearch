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
import org.elasticsearch.shield.transport.filter.IPFilter;
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
            if (clientMode) {
                ((TransportModule) module).setTransportService(SecuredClientTransportService.class, ShieldPlugin.NAME);
            } else {
                ((TransportModule) module).setTransportService(SecuredServerTransportService.class, ShieldPlugin.NAME);
            }
        }
    }

    @Override
    protected void configure(boolean clientMode) {
        if (clientMode) {
            // no ip filtering on the client
            bind(IPFilter.class).toProvider(Providers.<IPFilter>of(null));
            bind(ClientTransportFilter.class).to(ClientTransportFilter.TransportClient.class).asEagerSingleton();
        } else {
            bind(ClientTransportFilter.class).to(ClientTransportFilter.Node.class).asEagerSingleton();
            bind(ServerTransportFilter.ClientProfile.class).asEagerSingleton();
            bind(ServerTransportFilter.NodeProfile.class).asEagerSingleton();
            if (settings.getAsBoolean("shield.transport.filter.enabled", true)) {
                bind(IPFilter.class).asEagerSingleton();
            }
        }
    }
}
