/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.downsample.DownsampleShardPersistentTaskState;
import org.elasticsearch.xpack.core.downsample.DownsampleShardTask;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleOrchestrator;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore;
import org.elasticsearch.xpack.downsample.orchestration.ShardTaskPoller;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Downsample extends Plugin implements ActionPlugin, PersistentTaskPlugin, SystemIndexPlugin {

    public static final String DOWNSAMPLE_TASK_THREAD_POOL_NAME = "downsample_indexing";

    private final SetOnce<DownsampleOrchestrator> orchestrator = new SetOnce<>();
    private static final int DOWNSAMPLE_TASK_THREAD_POOL_QUEUE_SIZE = 256;

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
        RestHandlersServices restHandlersServices,
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
        DownsampleStateStore store = new DownsampleStateStore(services.client(), services.clusterService(), services.projectResolver());
        DownsampleOrchestrator orchestrator = new DownsampleOrchestrator(
            store,
            services.clusterService(),
            services.threadPool(),
            services.projectResolver(),
            services.clusterService().getSettings()
        );
        this.orchestrator.set(orchestrator);
        if (DownsampleStateIndex.FEATURE_FLAG.isEnabled()) {
            services.clusterService().addListener(orchestrator);
        }
        ShardTaskPoller poller = new ShardTaskPoller(
            services.client(),
            store,
            services.clusterService(),
            services.threadPool(),
            services.projectResolver(),
            DownsampleStateIndex.FEATURE_FLAG.isEnabled() && DiscoveryNode.canContainData(services.clusterService().getSettings())
        );
        return List.of(DownsampleMetrics.class, store, orchestrator, poller);
    }

    /**
     * The orchestrator, {@code null} until the components are created. For the {@link DownsamplingOperationsMonitor} extension.
     */
    @Nullable
    DownsampleOrchestrator orchestrator() {
        return orchestrator.get();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(DownsampleOrchestrator.RECONCILE_INTERVAL_SETTING, ShardTaskPoller.POLL_INTERVAL_SETTING);
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return DownsampleStateIndex.FEATURE_FLAG.isEnabled() ? List.of(DownsampleStateIndex.descriptor()) : List.of();
    }

    @Override
    public String getFeatureName() {
        return "downsample";
    }

    @Override
    public String getFeatureDescription() {
        return "Stores the state of downsample operations";
    }
}
