/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.downsample.DownsampleIndexerAction;

import java.util.List;
import java.util.function.Supplier;

public class Downsample extends Plugin implements ActionPlugin {

    public static final String DOWSAMPLE_TASK_THREAD_POOL_NAME = "downsample_indexing";
    private static final int DOWNSAMPLE_TASK_THREAD_POOL_QUEUE_SIZE = 256;

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final FixedExecutorBuilder downsample = new FixedExecutorBuilder(
            settings,
            DOWSAMPLE_TASK_THREAD_POOL_NAME,
            ThreadPool.oneEighthAllocatedProcessors(EsExecutors.allocatedProcessors(settings)),
            DOWNSAMPLE_TASK_THREAD_POOL_QUEUE_SIZE,
            "xpack.downsample.thread_pool",
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        return List.of(downsample);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(DownsampleIndexerAction.INSTANCE, TransportDownsampleIndexerAction.class),
            new ActionHandler<>(DownsampleAction.INSTANCE, TransportDownsampleAction.class)
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
        return List.of(new RestDownsampleAction());
    }
}
