/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;

public class TestConsumerPlugin extends AbstractPlugin {

    private final boolean isClient;

    private final Settings settings;

    @Inject
    public TestConsumerPlugin(Settings settings) {
        this.settings = settings;
        this.isClient = DiscoveryNode.clientNode(settings);
    }

    @Override
    public String name() {
        return "test_plugin";
    }

    @Override
    public String description() {
        return "test licensing consumer plugin";
    }


    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        if (!isClient) {
            services.add(TestPluginService.class);
        }
        return services;
    }
}
