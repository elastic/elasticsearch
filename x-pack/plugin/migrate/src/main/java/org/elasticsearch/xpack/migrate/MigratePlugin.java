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
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
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
import org.elasticsearch.xpack.migrate.action.CreateIndexFromSourceAction;
import org.elasticsearch.xpack.migrate.action.CreateIndexFromSourceTransportAction;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusTransportAction;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamIndexAction;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamIndexTransportAction;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamTransportAction;
import org.elasticsearch.xpack.migrate.rest.RestCancelReindexDataStreamAction;
import org.elasticsearch.xpack.migrate.rest.RestCreateIndexFromSourceAction;
import org.elasticsearch.xpack.migrate.rest.RestGetMigrationReindexStatusAction;
import org.elasticsearch.xpack.migrate.rest.RestMigrationReindexAction;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamPersistentTaskExecutor;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamPersistentTaskState;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamStatus;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTask;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTaskParams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.REINDEX_DATA_STREAM_ORIGIN;
import static org.elasticsearch.xpack.migrate.action.ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING;
import static org.elasticsearch.xpack.migrate.task.ReindexDataStreamPersistentTaskExecutor.MAX_CONCURRENT_INDICES_REINDEXED_PER_DATA_STREAM_SETTING;

public class MigratePlugin extends Plugin implements ActionPlugin, PersistentTaskPlugin {
    @Override
    public Collection<?> createComponents(PluginServices services) {
        var registry = new MigrateTemplateRegistry(
            services.environment().settings(),
            services.clusterService(),
            services.threadPool(),
            services.client(),
            services.xContentRegistry()
        );
        registry.initialize();
        return List.of(registry);
    }

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
        handlers.add(new RestMigrationReindexAction());
        handlers.add(new RestGetMigrationReindexStatusAction());
        handlers.add(new RestCancelReindexDataStreamAction());
        handlers.add(new RestCreateIndexFromSourceAction());
        return handlers;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(ReindexDataStreamAction.INSTANCE, ReindexDataStreamTransportAction.class));
        actions.add(new ActionHandler<>(GetMigrationReindexStatusAction.INSTANCE, GetMigrationReindexStatusTransportAction.class));
        actions.add(new ActionHandler<>(CancelReindexDataStreamAction.INSTANCE, CancelReindexDataStreamTransportAction.class));
        actions.add(new ActionHandler<>(ReindexDataStreamIndexAction.INSTANCE, ReindexDataStreamIndexTransportAction.class));
        actions.add(new ActionHandler<>(CreateIndexFromSourceAction.INSTANCE, CreateIndexFromSourceTransportAction.class));
        return actions;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
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
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
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
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return List.of(
            new ReindexDataStreamPersistentTaskExecutor(
                new OriginSettingClient(client, REINDEX_DATA_STREAM_ORIGIN),
                clusterService,
                ReindexDataStreamTask.TASK_NAME,
                threadPool
            )
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> pluginSettings = new ArrayList<>();
        pluginSettings.add(MAX_CONCURRENT_INDICES_REINDEXED_PER_DATA_STREAM_SETTING);
        pluginSettings.add(REINDEX_MAX_REQUESTS_PER_SECOND_SETTING);
        return pluginSettings;
    }
}
