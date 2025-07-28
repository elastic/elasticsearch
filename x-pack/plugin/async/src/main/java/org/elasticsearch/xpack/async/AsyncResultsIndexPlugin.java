/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.async;

import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class AsyncResultsIndexPlugin extends Plugin implements SystemIndexPlugin {

    protected final Settings settings;

    public AsyncResultsIndexPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings unused) {
        return List.of(AsyncTaskIndexService.getSystemIndexDescriptor());
    }

    @Override
    public String getFeatureName() {
        return "async_search";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages results of async searches";
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        List<Object> components = new ArrayList<>();
        if (DiscoveryNode.canContainData(services.environment().settings())) {
            // only data nodes should be eligible to run the maintenance service.
            AsyncTaskMaintenanceService maintenanceService = new AsyncTaskMaintenanceService(
                services.clusterService(),
                services.projectResolver(),
                services.nodeEnvironment().nodeId(),
                settings,
                services.threadPool(),
                new OriginSettingClient(services.client(), ASYNC_SEARCH_ORIGIN)
            );
            components.add(maintenanceService);
        }
        return components;
    }
}
