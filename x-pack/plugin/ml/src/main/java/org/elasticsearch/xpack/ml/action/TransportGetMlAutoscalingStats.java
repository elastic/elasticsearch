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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats.Request;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats.Response;
import org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingResourceTracker;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

/**
 * Internal (no-REST) transport to retrieve metrics for serverless autoscaling.
 */
public class TransportGetMlAutoscalingStats extends TransportMasterNodeAction<Request, Response> {

    private final MlMemoryTracker mlMemoryTracker;
    private final Settings settings;

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
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {

        if (mlMemoryTracker.isRecentlyRefreshed() == false) {
            // Recent memory statistics aren't available at the moment, trigger a refresh in the background.
            // (If a refresh is already in progress, this won't trigger a new one.) We still proceed to return a
            // scaling response in this case. This API gets called every 5 seconds, so the memory info is likely only
            // a few seconds stale, and still good enough. If it gets _really_ badly out of date, or has never been
            // populated in the first place then there are places in MlAutoscalingResourceTracker.getMlAutoscalingStats
            // that will return a no-scale.
            mlMemoryTracker.asyncRefresh();
        }

        MlAutoscalingResourceTracker.getMlAutoscalingStats(
            state,
            clusterService.getClusterSettings(),
            mlMemoryTracker,
            settings,
            listener.delegateFailureAndWrap((l, autoscalingResources) -> l.onResponse(new Response(autoscalingResources)))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
