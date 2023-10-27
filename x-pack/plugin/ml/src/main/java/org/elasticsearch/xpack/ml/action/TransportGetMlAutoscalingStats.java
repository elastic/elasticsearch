/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats.Request;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats.Response;
import org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingResourceTracker;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.concurrent.Executor;

/**
 * Internal (no-REST) transport to retrieve metrics for serverless autoscaling.
 */
public class TransportGetMlAutoscalingStats extends TransportMasterNodeAction<Request, Response> {

    private final MlMemoryTracker mlMemoryTracker;
    private final Settings settings;
    private final Executor timeoutExecutor;

    @Inject
    public TransportGetMlAutoscalingStats(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        MlMemoryTracker mlMemoryTracker
    ) {
        super(
            GetMlAutoscalingStats.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.mlMemoryTracker = mlMemoryTracker;
        this.settings = settings;
        this.timeoutExecutor = threadPool.generic();
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {

        if (mlMemoryTracker.isRecentlyRefreshed()) {
            MlAutoscalingResourceTracker.getMlAutoscalingStats(
                state,
                clusterService.getClusterSettings(),
                mlMemoryTracker,
                settings,
                ActionListener.wrap(autoscalingResources -> listener.onResponse(new Response(autoscalingResources)), listener::onFailure)
            );
        } else {
            // Recent memory statistics aren't available at the moment, trigger a refresh and return a no-scale.
            // (If a refresh is already in progress, this won't trigger a new one.)
            mlMemoryTracker.asyncRefresh();
            listener.onResponse(new Response(MlAutoscalingResourceTracker.noScaleStats(state)));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
