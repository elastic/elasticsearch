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

/**
 * Plugin for managing asynchronous search results in Elasticsearch.
 * <p>
 * This plugin provides functionality for storing and managing results from asynchronous search operations.
 * It maintains a system index for async task results and provides maintenance services for cleanup operations.
 * </p>
 */
public class AsyncResultsIndexPlugin extends Plugin implements SystemIndexPlugin {

    protected final Settings settings;

    /**
     * Constructs a new AsyncResultsIndexPlugin with the specified settings.
     *
     * @param settings the node settings used to configure the plugin
     */
    public AsyncResultsIndexPlugin(Settings settings) {
        this.settings = settings;
    }

    /**
     * Returns the system index descriptors for async search results storage.
     * <p>
     * This method provides the descriptor for the system index used to store asynchronous task results.
     * </p>
     *
     * @param unused the settings parameter (not used in this implementation)
     * @return a collection containing the system index descriptor for async task storage
     */
    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings unused) {
        return List.of(AsyncTaskIndexService.getSystemIndexDescriptor());
    }

    /**
     * Returns the feature name for this plugin.
     *
     * @return the feature name "async_search"
     */
    @Override
    public String getFeatureName() {
        return "async_search";
    }

    /**
     * Returns a human-readable description of this plugin's feature.
     *
     * @return a description indicating this plugin manages async search results
     */
    @Override
    public String getFeatureDescription() {
        return "Manages results of async searches";
    }

    /**
     * Creates and returns the plugin components.
     * <p>
     * On data nodes, this creates an {@link AsyncTaskMaintenanceService} responsible for
     * cleaning up expired async search results. Non-data nodes do not run the maintenance service.
     * </p>
     *
     * @param services the plugin services providing access to cluster resources
     * @return a collection of components; contains the maintenance service on data nodes, empty otherwise
     */
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
