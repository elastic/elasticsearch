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

public class Graph extends Plugin implements ActionPlugin {

    public static final LicensedFeature.Momentary GRAPH_FEATURE = LicensedFeature.momentary(null, "graph", License.OperationMode.PLATINUM);

    protected final boolean enabled;

    public Graph(Settings settings) {
        this.enabled = XPackSettings.GRAPH_ENABLED.get(settings);
    }

    @Override
    public List<ActionHandler> getActions() {
        var usageAction = new ActionHandler(XPackUsageFeatureAction.GRAPH, GraphUsageTransportAction.class);
        var infoAction = new ActionHandler(XPackInfoFeatureAction.GRAPH, GraphInfoTransportAction.class);
        if (false == enabled) {
            return Arrays.asList(usageAction, infoAction);
        }
        return Arrays.asList(new ActionHandler(GraphExploreAction.INSTANCE, TransportGraphExploreAction.class), usageAction, infoAction);
    }

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
