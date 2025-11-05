/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.graph;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.graph.action.TransportGraphExploreAction;
import org.elasticsearch.xpack.graph.rest.action.RestGraphAction;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Plugin for Graph exploration functionality in Elasticsearch.
 * <p>
 * This plugin provides the Graph API, which enables exploration of relationships
 * in data through relevance-based graph analysis. Graph can discover how items
 * are related using the {@code _explore} API endpoint. This feature requires a
 * Platinum or Enterprise license.
 * </p>
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * POST /my-index/_graph/explore
 * {
 *   "query": {
 *     "match": {
 *       "field": "value"
 *     }
 *   },
 *   "vertices": [
 *     {
 *       "field": "user"
 *     }
 *   ],
 *   "connections": {
 *     "vertices": [
 *       {
 *         "field": "product"
 *       }
 *     ]
 *   }
 * }
 * }</pre>
 */
public class Graph extends Plugin implements ActionPlugin {

    /**
     * Licensed feature definition for Graph functionality.
     * Requires a Platinum or Enterprise license.
     */
    public static final LicensedFeature.Momentary GRAPH_FEATURE = LicensedFeature.momentary(null, "graph", License.OperationMode.PLATINUM);

    protected final boolean enabled;

    /**
     * Constructs a new Graph plugin with the specified settings.
     *
     * @param settings the node settings used to determine if Graph is enabled
     */
    public Graph(Settings settings) {
        this.enabled = XPackSettings.GRAPH_ENABLED.get(settings);
    }

    /**
     * Returns the list of action handlers provided by this plugin.
     * <p>
     * Registers the Graph explore action along with usage and info actions.
     * If the plugin is disabled, only the usage and info actions are registered.
     * </p>
     *
     * @return a list of action handlers for graph operations
     */
    @Override
    public List<ActionHandler> getActions() {
        var usageAction = new ActionHandler(XPackUsageFeatureAction.GRAPH, GraphUsageTransportAction.class);
        var infoAction = new ActionHandler(XPackInfoFeatureAction.GRAPH, GraphInfoTransportAction.class);
        if (false == enabled) {
            return Arrays.asList(usageAction, infoAction);
        }
        return Arrays.asList(new ActionHandler(GraphExploreAction.INSTANCE, TransportGraphExploreAction.class), usageAction, infoAction);
    }

    /**
     * Returns the REST handlers provided by this plugin.
     * <p>
     * Registers the REST endpoint for graph exploration at {@code /_graph/explore}.
     * If the plugin is disabled, no REST handlers are registered.
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
     * @return a list containing the graph REST handler if enabled, empty otherwise
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
        if (false == enabled) {
            return emptyList();
        }
        return singletonList(new RestGraphAction());
    }
}
