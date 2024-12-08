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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamTransportAction;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamPersistentTaskExecutor;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamPersistentTaskState;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamStatus;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTask;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTaskParams;

import java.util.ArrayList;
import java.util.List;

public class MigratePlugin extends Plugin implements ActionPlugin, PersistentTaskPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(ReindexDataStreamAction.INSTANCE, ReindexDataStreamTransportAction.class));
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
        return List.of(new ReindexDataStreamPersistentTaskExecutor(client, clusterService, ReindexDataStreamTask.TASK_NAME, threadPool));
    }
}
