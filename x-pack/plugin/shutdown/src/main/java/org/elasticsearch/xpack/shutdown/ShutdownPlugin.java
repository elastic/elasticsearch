/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Plugin for node shutdown management in Elasticsearch.
 * <p>
 * This plugin provides APIs for gracefully shutting down nodes in a cluster. It allows
 * administrators to mark nodes for shutdown, which triggers the cluster to prepare for the
 * node's removal by relocating shards, stopping allocations, and ensuring data safety.
 * </p>
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * // Mark a node for shutdown
 * PUT /_nodes/<node_id>/shutdown
 * {
 *   "type": "restart",
 *   "reason": "Planned maintenance",
 *   "allocation_delay": "10m"
 * }
 *
 * // Get shutdown status
 * GET /_nodes/<node_id>/shutdown
 *
 * // Cancel shutdown
 * DELETE /_nodes/<node_id>/shutdown
 * }</pre>
 */
public class ShutdownPlugin extends Plugin implements ActionPlugin {
    /**
     * Creates the plugin components.
     * <p>
     * Initializes the {@link NodeSeenService} which tracks when nodes are last seen
     * in the cluster, helping coordinate graceful shutdowns.
     * </p>
     *
     * @param services the plugin services providing access to cluster resources
     * @return a collection containing the node seen service
     */
    @Override
    public Collection<?> createComponents(PluginServices services) {

        NodeSeenService nodeSeenService = new NodeSeenService(services.clusterService());

        return Collections.singletonList(nodeSeenService);
    }

    /**
     * Returns the list of action handlers provided by this plugin.
     * <p>
     * Registers transport actions for putting, deleting, and getting shutdown status
     * for nodes in the cluster.
     * </p>
     *
     * @return a list of action handlers for shutdown operations
     */
    @Override
    public List<ActionHandler> getActions() {
        ActionHandler putShutdown = new ActionHandler(PutShutdownNodeAction.INSTANCE, TransportPutShutdownNodeAction.class);
        ActionHandler deleteShutdown = new ActionHandler(DeleteShutdownNodeAction.INSTANCE, TransportDeleteShutdownNodeAction.class);
        ActionHandler getStatus = new ActionHandler(GetShutdownStatusAction.INSTANCE, TransportGetShutdownStatusAction.class);
        return Arrays.asList(putShutdown, deleteShutdown, getStatus);
    }

    /**
     * Returns the REST handlers provided by this plugin.
     * <p>
     * Registers REST endpoints for node shutdown management at
     * {@code /_nodes/<node_id>/shutdown}.
     * </p>
     *
     * @param settings the node settings
     * @param namedWriteableRegistry the named writeable registry
     * @param restController the REST controller
     * @param clusterSettings the cluster settings
     * @param indexScopedSettings the index-scoped settings
     * @param settingsFilter the settings filter
     * @param indexNameExpressionResolver the index name expression resolver
     * @param nodesInCluster supplier for discovery nodes
     * @param clusterSupportsFeature predicate to check feature support
     * @return a list of REST handlers for shutdown endpoints
     */
    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return Arrays.asList(new RestPutShutdownNodeAction(), new RestDeleteShutdownNodeAction(), new RestGetShutdownStatusAction());
    }
}
