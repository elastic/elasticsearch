/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.consumer;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;

public abstract class TestConsumerPluginBase extends Plugin {

    private final boolean isEnabled;

    public TestConsumerPluginBase(Settings settings) {
        if (DiscoveryNode.clientNode(settings)) {
            // Enable plugin only on node clients
            this.isEnabled = "node".equals(settings.get(Client.CLIENT_TYPE_SETTING));
        } else {
            this.isEnabled = true;
        }
    }

    @Override
    public String name() {
        return pluginName();
    }

    @Override
    public String description() {
        return "test licensing consumer plugin";
    }


    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        if (isEnabled) {
            services.add(service());
        }
        return services;
    }

    public abstract Class<? extends TestPluginServiceBase> service();

    protected abstract String pluginName();

    public abstract String id();
}
