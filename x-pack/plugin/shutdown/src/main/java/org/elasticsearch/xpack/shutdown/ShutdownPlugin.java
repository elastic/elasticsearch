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

public class ShutdownPlugin extends Plugin implements ActionPlugin {
    @Override
    public Collection<?> createComponents(PluginServices services) {

        NodeSeenService nodeSeenService = new NodeSeenService(services.clusterService());

        return Collections.singletonList(nodeSeenService);
    }

    @Override
    public List<ActionHandler> getActions() {
        ActionHandler putShutdown = new ActionHandler(PutShutdownNodeAction.INSTANCE, TransportPutShutdownNodeAction.class);
        ActionHandler deleteShutdown = new ActionHandler(DeleteShutdownNodeAction.INSTANCE, TransportDeleteShutdownNodeAction.class);
        ActionHandler getStatus = new ActionHandler(GetShutdownStatusAction.INSTANCE, TransportGetShutdownStatusAction.class);
        return Arrays.asList(putShutdown, deleteShutdown, getStatus);
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
        return Arrays.asList(new RestPutShutdownNodeAction(), new RestDeleteShutdownNodeAction(), new RestGetShutdownStatusAction());
    }
}
