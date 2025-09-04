/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.tasks.Task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class ReindexPlugin extends Plugin implements ActionPlugin {
    public static final String NAME = "reindex";

    public static final ActionType<ListTasksResponse> RETHROTTLE_ACTION = new ActionType<>("cluster:admin/reindex/rethrottle");

    @Override
    public List<ActionHandler> getActions() {
        return Arrays.asList(
            new ActionHandler(ReindexAction.INSTANCE, TransportReindexAction.class),
            new ActionHandler(UpdateByQueryAction.INSTANCE, TransportUpdateByQueryAction.class),
            new ActionHandler(DeleteByQueryAction.INSTANCE, TransportDeleteByQueryAction.class),
            new ActionHandler(RETHROTTLE_ACTION, TransportRethrottleAction.class)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return singletonList(
            new NamedWriteableRegistry.Entry(Task.Status.class, BulkByScrollTask.Status.NAME, BulkByScrollTask.Status::new)
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
        return Arrays.asList(
            new RestReindexAction(clusterSupportsFeature),
            new RestUpdateByQueryAction(clusterSupportsFeature),
            new RestDeleteByQueryAction(clusterSupportsFeature),
            new RestRethrottleAction(nodesInCluster)
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        return List.of(
            new ReindexSslConfig(services.environment().settings(), services.environment(), services.resourceWatcherService()),
            new ReindexMetrics(services.telemetryProvider().getMeterRegistry()),
            new UpdateByQueryMetrics(services.telemetryProvider().getMeterRegistry()),
            new DeleteByQueryMetrics(services.telemetryProvider().getMeterRegistry())
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        final List<Setting<?>> settings = new ArrayList<>();
        settings.add(TransportReindexAction.REMOTE_CLUSTER_WHITELIST);
        settings.addAll(ReindexSslConfig.getSettings());
        return settings;
    }
}
