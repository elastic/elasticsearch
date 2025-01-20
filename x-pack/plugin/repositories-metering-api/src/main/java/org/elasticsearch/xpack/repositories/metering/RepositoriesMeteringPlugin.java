/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.repositories.metering;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
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
import org.elasticsearch.xpack.repositories.metering.action.ClearRepositoriesMeteringArchiveAction;
import org.elasticsearch.xpack.repositories.metering.action.RepositoriesMeteringAction;
import org.elasticsearch.xpack.repositories.metering.action.TransportClearRepositoriesStatsArchiveAction;
import org.elasticsearch.xpack.repositories.metering.action.TransportRepositoriesStatsAction;
import org.elasticsearch.xpack.repositories.metering.rest.RestClearRepositoriesMeteringArchiveAction;
import org.elasticsearch.xpack.repositories.metering.rest.RestGetRepositoriesMeteringAction;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class RepositoriesMeteringPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(RepositoriesMeteringAction.INSTANCE, TransportRepositoriesStatsAction.class),
            new ActionHandler<>(ClearRepositoriesMeteringArchiveAction.INSTANCE, TransportClearRepositoriesStatsArchiveAction.class)
        );
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
        return List.of(new RestGetRepositoriesMeteringAction(), new RestClearRepositoriesMeteringArchiveAction());
    }
}
