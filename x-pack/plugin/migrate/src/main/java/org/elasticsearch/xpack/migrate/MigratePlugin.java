/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.migrate.action.CancelReindexDataStreamAction;
import org.elasticsearch.xpack.migrate.action.CancelReindexDataStreamTransportAction;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusTransportAction;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamTransportAction;
import org.elasticsearch.xpack.migrate.rest.RestCancelReindexDataStreamAction;
import org.elasticsearch.xpack.migrate.rest.RestGetMigrationReindexStatusAction;
import org.elasticsearch.xpack.migrate.rest.RestMigrationReindexAction;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamPersistentTaskExecutor;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamPersistentTaskState;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamStatus;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTask;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTaskParams;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction.REINDEX_DATA_STREAM_FEATURE_FLAG;

public class MigratePlugin extends Plugin implements ActionPlugin, PersistentTaskPlugin {

    @Override
    public List<RestHandler> getRestHandlers(
        Settings unused,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        List<RestHandler> handlers = new ArrayList<>();
        if (REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled()) {
            handlers.add(new RestMigrationReindexAction());
            handlers.add(new RestGetMigrationReindexStatusAction());
            handlers.add(new RestCancelReindexDataStreamAction());
        }
        return handlers;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        if (REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled()) {
            actions.add(new ActionHandler<>(ReindexDataStreamAction.INSTANCE, ReindexDataStreamTransportAction.class));
            actions.add(new ActionHandler<>(GetMigrationReindexStatusAction.INSTANCE, GetMigrationReindexStatusTransportAction.class));
            actions.add(new ActionHandler<>(CancelReindexDataStreamAction.INSTANCE, CancelReindexDataStreamTransportAction.class));
        }
        return actions;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        if (REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled()) {
            return List.of(
                new NamedXContentRegistry.Entry(
                    PersistentTaskState.class,
                    new ParseField(ReindexDataStreamPersistentTaskState.NAME),
                    ReindexDataStreamPersistentTaskState::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(ReindexDataStreamTaskParams.NAME),
                    ReindexDataStreamTaskParams::fromXContent
                )
            );
        } else {
            return List.of();
        }
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        if (REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled()) {
            return List.of(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskState.class,
                    ReindexDataStreamPersistentTaskState.NAME,
                    ReindexDataStreamPersistentTaskState::new
                ),
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    ReindexDataStreamTaskParams.NAME,
                    ReindexDataStreamTaskParams::new
                ),
                new NamedWriteableRegistry.Entry(Task.Status.class, ReindexDataStreamStatus.NAME, ReindexDataStreamStatus::new)
            );
        } else {
            return List.of();
        }
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        if (REINDEX_DATA_STREAM_FEATURE_FLAG.isEnabled()) {
            return List.of(
                new ReindexDataStreamPersistentTaskExecutor(client, clusterService, ReindexDataStreamTask.TASK_NAME, threadPool)
            );
        } else {
            return List.of();
        }
    }
}
