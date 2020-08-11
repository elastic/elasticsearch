/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.metrics;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.repositories.metrics.action.ClearRepositoriesMetricsArchiveAction;
import org.elasticsearch.xpack.repositories.metrics.action.RepositoriesMetricsAction;
import org.elasticsearch.xpack.repositories.metrics.action.TransportClearRepositoriesStatsArchiveAction;
import org.elasticsearch.xpack.repositories.metrics.action.TransportRepositoriesStatsAction;
import org.elasticsearch.xpack.repositories.metrics.rest.RestClearRepositoriesMetricsArchiveAction;
import org.elasticsearch.xpack.repositories.metrics.rest.RestGetRepositoriesMetricsAction;

import java.util.List;
import java.util.function.Supplier;

public final class RepositoriesMeteringPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(RepositoriesMetricsAction.INSTANCE, TransportRepositoriesStatsAction.class),
            new ActionHandler<>(ClearRepositoriesMetricsArchiveAction.INSTANCE, TransportClearRepositoriesStatsArchiveAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestGetRepositoriesMetricsAction(), new RestClearRepositoriesMetricsArchiveAction());
    }
}
