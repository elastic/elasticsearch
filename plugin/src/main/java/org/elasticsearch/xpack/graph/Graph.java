/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.graph;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.graph.action.TransportGraphExploreAction;
import org.elasticsearch.xpack.graph.rest.action.RestGraphAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.XpackField.GRAPH;

public class Graph implements ActionPlugin {

    public static final String NAME = "graph";
    protected final boolean enabled;
    
    
    public Graph(Settings settings) {
        this.enabled = XPackSettings.GRAPH_ENABLED.get(settings);
    }

    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(b -> {
            XPackPlugin.bindFeatureSet(b, GraphFeatureSet.class);
        });
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (false == enabled) {
            return emptyList();
        }
        return singletonList(new ActionHandler<>(GraphExploreAction.INSTANCE, TransportGraphExploreAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        if (false == enabled) {
            return emptyList();
        }
        return singletonList(new RestGraphAction(settings, restController));
    }

    public static Collection<? extends NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, GRAPH, GraphFeatureSet.Usage::new));
    }
}
