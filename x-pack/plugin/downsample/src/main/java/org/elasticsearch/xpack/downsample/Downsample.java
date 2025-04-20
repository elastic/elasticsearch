/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.downsample.DownsampleAction;
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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.downsample.DownsampleShardPersistentTaskState;
import org.elasticsearch.xpack.core.downsample.DownsampleShardTask;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Downsample extends Plugin implements ActionPlugin, PersistentTaskPlugin {

    public static final String DOWNSAMPLE_TASK_THREAD_POOL_NAME = "downsample_indexing";
    private static final int DOWNSAMPLE_TASK_THREAD_POOL_QUEUE_SIZE = 256;
    public static final String DOWNSAMPLE_MIN_NUMBER_OF_REPLICAS_NAME = "downsample.min_number_of_replicas";

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final FixedExecutorBuilder downsample = new FixedExecutorBuilder(
            settings,
            DOWNSAMPLE_TASK_THREAD_POOL_NAME,
            ThreadPool.oneEighthAllocatedProcessors(EsExecutors.allocatedProcessors(settings)),
            DOWNSAMPLE_TASK_THREAD_POOL_QUEUE_SIZE,
            "xpack.downsample.thread_pool",
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        return List.of(downsample);
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(DownsampleAction.INSTANCE, TransportDownsampleAction.class),
            new ActionHandler(
                DownsampleShardPersistentTaskExecutor.DelegatingAction.INSTANCE,
                DownsampleShardPersistentTaskExecutor.DelegatingAction.TA.class
            )
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
        return List.of(new RestDownsampleAction());
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
            new DownsampleShardPersistentTaskExecutor(
                client,
                DownsampleShardTask.TASK_NAME,
                threadPool.executor(DOWNSAMPLE_TASK_THREAD_POOL_NAME)
            )
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                PersistentTaskState.class,
                new ParseField(DownsampleShardPersistentTaskState.NAME),
                DownsampleShardPersistentTaskState::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(DownsampleShardTaskParams.NAME),
                DownsampleShardTaskParams::fromXContent
            )
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                PersistentTaskState.class,
                DownsampleShardPersistentTaskState.NAME,
                DownsampleShardPersistentTaskState::readFromStream
            ),
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, DownsampleShardTaskParams.NAME, DownsampleShardTaskParams::new)
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        return List.of(DownsampleMetrics.class);
    }
}
