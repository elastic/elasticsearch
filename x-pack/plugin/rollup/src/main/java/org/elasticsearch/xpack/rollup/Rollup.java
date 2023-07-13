/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupCapsAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.core.rollup.action.RollupShardPersistentTaskState;
import org.elasticsearch.xpack.core.rollup.action.RollupShardTask;
import org.elasticsearch.xpack.core.rollup.action.StartRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;
import org.elasticsearch.xpack.downsample.RestDownsampleAction;
import org.elasticsearch.xpack.downsample.RollupShardPersistentTaskExecutor;
import org.elasticsearch.xpack.downsample.RollupShardTaskParams;
import org.elasticsearch.xpack.downsample.TransportDownsampleAction;
import org.elasticsearch.xpack.rollup.action.TransportDeleteRollupJobAction;
import org.elasticsearch.xpack.rollup.action.TransportGetRollupCapsAction;
import org.elasticsearch.xpack.rollup.action.TransportGetRollupIndexCapsAction;
import org.elasticsearch.xpack.rollup.action.TransportGetRollupJobAction;
import org.elasticsearch.xpack.rollup.action.TransportPutRollupJobAction;
import org.elasticsearch.xpack.rollup.action.TransportRollupSearchAction;
import org.elasticsearch.xpack.rollup.action.TransportStartRollupAction;
import org.elasticsearch.xpack.rollup.action.TransportStopRollupAction;
import org.elasticsearch.xpack.rollup.job.RollupJobTask;
import org.elasticsearch.xpack.rollup.rest.RestDeleteRollupJobAction;
import org.elasticsearch.xpack.rollup.rest.RestGetRollupCapsAction;
import org.elasticsearch.xpack.rollup.rest.RestGetRollupIndexCapsAction;
import org.elasticsearch.xpack.rollup.rest.RestGetRollupJobsAction;
import org.elasticsearch.xpack.rollup.rest.RestPutRollupJobAction;
import org.elasticsearch.xpack.rollup.rest.RestRollupSearchAction;
import org.elasticsearch.xpack.rollup.rest.RestStartRollupJobAction;
import org.elasticsearch.xpack.rollup.rest.RestStopRollupJobAction;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class Rollup extends Plugin implements ActionPlugin, PersistentTaskPlugin {

    // Introduced in ES version 6.3
    public static final int ROLLUP_VERSION_V1 = 1;
    // Introduced in ES Version 6.4
    // Bumped due to ID collision, see #32372
    public static final int ROLLUP_VERSION_V2 = 2;
    public static final int CURRENT_ROLLUP_VERSION = ROLLUP_VERSION_V2;

    public static final String TASK_THREAD_POOL_NAME = RollupField.NAME + "_indexing";
    public static final String DOWSAMPLE_TASK_THREAD_POOL_NAME = "downsample_indexing";
    public static final int DOWNSAMPLE_TASK_THREAD_POOL_QUEUE_SIZE = 256;

    public static final String ROLLUP_TEMPLATE_VERSION_FIELD = "rollup-version";

    private final SetOnce<SchedulerEngine> schedulerEngine = new SetOnce<>();
    private final Settings settings;
    private IndicesService indicesService;

    public Rollup(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings unused,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return Arrays.asList(
            new RestRollupSearchAction(),
            new RestPutRollupJobAction(),
            new RestStartRollupJobAction(),
            new RestStopRollupJobAction(),
            new RestDeleteRollupJobAction(),
            new RestGetRollupJobsAction(),
            new RestGetRollupCapsAction(),
            new RestGetRollupIndexCapsAction(),
            new RestDownsampleAction() // TSDB Downsampling
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(RollupSearchAction.INSTANCE, TransportRollupSearchAction.class),
            new ActionHandler<>(PutRollupJobAction.INSTANCE, TransportPutRollupJobAction.class),
            new ActionHandler<>(StartRollupJobAction.INSTANCE, TransportStartRollupAction.class),
            new ActionHandler<>(StopRollupJobAction.INSTANCE, TransportStopRollupAction.class),
            new ActionHandler<>(DeleteRollupJobAction.INSTANCE, TransportDeleteRollupJobAction.class),
            new ActionHandler<>(GetRollupJobsAction.INSTANCE, TransportGetRollupJobAction.class),
            new ActionHandler<>(GetRollupCapsAction.INSTANCE, TransportGetRollupCapsAction.class),
            new ActionHandler<>(GetRollupIndexCapsAction.INSTANCE, TransportGetRollupIndexCapsAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.ROLLUP, RollupUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.ROLLUP, RollupInfoTransportAction.class),
            new ActionHandler<>(DownsampleAction.INSTANCE, TransportDownsampleAction.class)
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settingsToUse) {
        final FixedExecutorBuilder rollup = new FixedExecutorBuilder(
            settingsToUse,
            Rollup.TASK_THREAD_POOL_NAME,
            1,
            -1,
            "xpack.rollup.task_thread_pool",
            false
        );

        final FixedExecutorBuilder downsample = new FixedExecutorBuilder(
            settingsToUse,
            Rollup.DOWSAMPLE_TASK_THREAD_POOL_NAME,
            ThreadPool.searchOrGetThreadPoolSize(EsExecutors.allocatedProcessors(settingsToUse)),
            Rollup.DOWNSAMPLE_TASK_THREAD_POOL_QUEUE_SIZE,
            "xpack.downsample.thread_pool",
            false
        );

        return List.of(rollup, downsample);
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        schedulerEngine.set(new SchedulerEngine(settings, getClock()));
        return List.of(
            new RollupJobTask.RollupJobPersistentTasksExecutor(client, schedulerEngine.get(), threadPool),
            new RollupShardPersistentTaskExecutor(
                client,
                this.indicesService,
                RollupShardTask.TASK_NAME,
                Rollup.DOWSAMPLE_TASK_THREAD_POOL_NAME
            )
        );
    }

    // overridable by tests
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    @Override
    public void close() {
        if (schedulerEngine.get() != null) {
            schedulerEngine.get().stop();
        }
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                PersistentTaskState.class,
                new ParseField(RollupShardPersistentTaskState.NAME),
                RollupShardPersistentTaskState::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(RollupShardTaskParams.NAME),
                RollupShardTaskParams::fromXContent
            )
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                RollupShardPersistentTaskState.class,
                RollupShardPersistentTaskState.NAME,
                RollupShardPersistentTaskState::readFromStream
            ),
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, RollupShardTaskParams.NAME, RollupShardTaskParams::readFromStream)
        );
    }

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry namedWriteableRegistry,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier,
        final Tracer tracer,
        final AllocationService allocationService,
        final IndicesService indicesService
    ) {
        final Collection<Object> components = super.createComponents(
            client,
            clusterService,
            threadPool,
            resourceWatcherService,
            scriptService,
            xContentRegistry,
            environment,
            nodeEnvironment,
            namedWriteableRegistry,
            indexNameExpressionResolver,
            repositoriesServiceSupplier,
            tracer,
            allocationService,
            indicesService
        );

        this.indicesService = indicesService;
        return components;
    }
}
