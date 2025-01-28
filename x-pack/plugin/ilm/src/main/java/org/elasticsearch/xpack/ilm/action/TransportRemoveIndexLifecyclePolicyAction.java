/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.Index;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction.Request;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction.Response;
import org.elasticsearch.xpack.ilm.IndexLifecycleTransition;

import java.util.ArrayList;
import java.util.List;

public class TransportRemoveIndexLifecyclePolicyAction extends TransportMasterNodeAction<Request, Response> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportRemoveIndexLifecyclePolicyAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            RemoveIndexLifecyclePolicyAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        final Index[] indices = indexNameExpressionResolver.concreteIndices(state, request.indicesOptions(), true, request.indices());
        submitUnbatchedTask("remove-lifecycle-for-index", new ClusterStateUpdateTask(request.masterNodeTimeout()) {

            private final List<String> failedIndexes = new ArrayList<>();

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return IndexLifecycleTransition.removePolicyForIndexes(indices, currentState, failedIndexes);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(new Response(failedIndexes));
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

}
