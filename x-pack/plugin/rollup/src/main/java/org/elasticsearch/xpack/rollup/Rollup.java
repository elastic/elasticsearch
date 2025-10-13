/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupCapsAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.core.rollup.action.StartRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;
import org.elasticsearch.xpack.rollup.action.RollupInfoTransportAction;
import org.elasticsearch.xpack.rollup.action.RollupUsageTransportAction;
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
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Rollup extends Plugin implements ActionPlugin, PersistentTaskPlugin {

    public static final String DEPRECATION_MESSAGE =
        "The rollup functionality will be removed in Elasticsearch 10.0. See docs for more information.";
    public static final String DEPRECATION_KEY = "rollup_removal";

    // Introduced in ES version 6.3
    public static final int ROLLUP_VERSION_V1 = 1;
    // Introduced in ES Version 6.4
    // Bumped due to ID collision, see #32372
    public static final int ROLLUP_VERSION_V2 = 2;
    public static final int CURRENT_ROLLUP_VERSION = ROLLUP_VERSION_V2;

    public static final String TASK_THREAD_POOL_NAME = RollupField.NAME + "_indexing";

    private final SetOnce<SchedulerEngine> schedulerEngine = new SetOnce<>();
    private final Settings settings;

    public Rollup(Settings settings) {
        this.settings = settings;
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
        return Arrays.asList(
            new RestRollupSearchAction(clusterSupportsFeature),
            new RestPutRollupJobAction(),
            new RestStartRollupJobAction(),
            new RestStopRollupJobAction(),
            new RestDeleteRollupJobAction(),
            new RestGetRollupJobsAction(),
            new RestGetRollupCapsAction(),
            new RestGetRollupIndexCapsAction()
        );
    }

    @Override
    public List<ActionHandler> getActions() {
        return Arrays.asList(
            new ActionHandler(RollupSearchAction.INSTANCE, TransportRollupSearchAction.class),
            new ActionHandler(PutRollupJobAction.INSTANCE, TransportPutRollupJobAction.class),
            new ActionHandler(StartRollupJobAction.INSTANCE, TransportStartRollupAction.class),
            new ActionHandler(StopRollupJobAction.INSTANCE, TransportStopRollupAction.class),
            new ActionHandler(DeleteRollupJobAction.INSTANCE, TransportDeleteRollupJobAction.class),
            new ActionHandler(GetRollupJobsAction.INSTANCE, TransportGetRollupJobAction.class),
            new ActionHandler(GetRollupCapsAction.INSTANCE, TransportGetRollupCapsAction.class),
            new ActionHandler(GetRollupIndexCapsAction.INSTANCE, TransportGetRollupIndexCapsAction.class),
            new ActionHandler(XPackUsageFeatureAction.ROLLUP, RollupUsageTransportAction.class),
            new ActionHandler(XPackInfoFeatureAction.ROLLUP, RollupInfoTransportAction.class)
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
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        return List.of(rollup);
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
        return List.of(new RollupJobTask.RollupJobPersistentTasksExecutor(client, schedulerEngine.get(), threadPool));
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
}
