/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.repositories.stats;

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
import org.elasticsearch.xpack.repositories.stats.action.ClearRepositoriesStatsArchiveAction;
import org.elasticsearch.xpack.repositories.stats.action.RepositoriesStatsAction;
import org.elasticsearch.xpack.repositories.stats.action.TransportClearRepositoriesStatsArchiveAction;
import org.elasticsearch.xpack.repositories.stats.action.TransportRepositoriesStatsAction;
import org.elasticsearch.xpack.repositories.stats.rest.RestClearRepositoriesStatsArchiveAction;
import org.elasticsearch.xpack.repositories.stats.rest.RestGetRepositoriesStatsAction;

import java.util.List;
import java.util.function.Supplier;

public final class RepositoriesStatsPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(RepositoriesStatsAction.INSTANCE, TransportRepositoriesStatsAction.class),
            new ActionHandler<>(ClearRepositoriesStatsArchiveAction.INSTANCE, TransportClearRepositoriesStatsArchiveAction.class)
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

        return List.of(new RestGetRepositoriesStatsAction(), new RestClearRepositoriesStatsArchiveAction());
    }
}
