/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.support.AbstractShieldModule;
import org.elasticsearch.shield.transport.filter.IPFilter;

/**
 *
 */
public class ShieldTransportModule extends AbstractShieldModule {

    public ShieldTransportModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configure(boolean clientMode) {
        if (clientMode) {
            // no ip filtering on the client
            bind(IPFilter.class).toProvider(Providers.<IPFilter>of(null));
            bind(ClientTransportFilter.class).to(ClientTransportFilter.TransportClient.class).asEagerSingleton();
        } else {
            bind(ClientTransportFilter.class).to(ClientTransportFilter.Node.class).asEagerSingleton();
            if (IPFilter.IP_FILTER_ENABLED_SETTING.get(settings)) {
                bind(IPFilter.class).asEagerSingleton();
            }
        }
    }
}
