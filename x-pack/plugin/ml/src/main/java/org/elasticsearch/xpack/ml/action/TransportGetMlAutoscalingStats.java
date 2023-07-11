/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
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

    private final Client client;
    private final MlMemoryTracker mlMemoryTracker;

    @Inject
    public TransportGetMlAutoscalingStats(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
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
            ThreadPool.Names.SAME
        );
        this.client = client;
        this.mlMemoryTracker = mlMemoryTracker;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        ParentTaskAssigningClient parentTaskAssigningClient = new ParentTaskAssigningClient(client, parentTaskId);

        if (mlMemoryTracker.isRecentlyRefreshed()) {
            MlAutoscalingResourceTracker.getMlAutoscalingStats(
                state,
                parentTaskAssigningClient,
                request.timeout(),
                mlMemoryTracker,
                listener.delegateFailureAndWrap((l, autoscalingResources) -> l.onResponse(new Response(autoscalingResources)))
            );
        } else {
            // recent memory statistics aren't available at the moment, trigger a refresh,
            // if a refresh has been triggered before, this will wait until refresh has happened
            // on busy cluster with many jobs this could take a while, therefore timeout and return a 408 in case
            mlMemoryTracker.refresh(
                state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE),
                ListenerTimeouts.wrapWithTimeout(
                    threadPool,
                    request.timeout(),
                    ThreadPool.Names.GENERIC,
                    listener.delegateFailureAndWrap(
                        (delegate, ignored) -> MlAutoscalingResourceTracker.getMlAutoscalingStats(
                            state,
                            parentTaskAssigningClient,
                            request.timeout(),
                            mlMemoryTracker,
                            delegate.delegateFailureAndWrap((l, autoscalingResources) -> l.onResponse(new Response(autoscalingResources)))
                        )
                    ),
                    timeoutTrigger -> {
                        // Timeout triggered
                        listener.onFailure(
                            new ElasticsearchStatusException(
                                "ML autoscaling metrics could not be retrieved in time, but should be available shortly.",
                                RestStatus.REQUEST_TIMEOUT
                            )
                        );
                    }
                )
            );
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
