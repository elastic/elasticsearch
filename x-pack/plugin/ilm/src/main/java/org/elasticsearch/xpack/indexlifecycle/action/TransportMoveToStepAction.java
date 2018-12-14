/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepAction.Request;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepAction.Response;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleService;

public class TransportMoveToStepAction extends TransportMasterNodeAction<Request, Response> {
    IndexLifecycleService indexLifecycleService;
    @Inject
    public TransportMoveToStepAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                     IndexLifecycleService indexLifecycleService) {
        super(MoveToStepAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            Request::new);
        this.indexLifecycleService = indexLifecycleService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected Response newResponse() {
        return new Response();
    }

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) {
        IndexMetaData indexMetaData = state.metaData().index(request.getIndex());
        if (indexMetaData == null) {
            listener.onFailure(new IllegalArgumentException("index [" + request.getIndex() + "] does not exist"));
            return;
        }
        clusterService.submitStateUpdateTask("index[" + request.getIndex() + "]-move-to-step",
            new AckedClusterStateUpdateTask<Response>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return indexLifecycleService.moveClusterStateToStep(currentState, request.getIndex(), request.getCurrentStepKey(),
                        request.getNextStepKey());
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    IndexMetaData newIndexMetaData = newState.metaData().index(indexMetaData.getIndex());
                    if (newIndexMetaData == null) {
                        // The index has somehow been deleted - there shouldn't be any opportunity for this to happen, but just in case.
                        logger.debug("index [" + indexMetaData.getIndex() + "] has been deleted after moving to step [" +
                            request.getNextStepKey() + "], skipping async action check");
                        return;
                    }
                    indexLifecycleService.maybeRunAsyncAction(newState, newIndexMetaData, request.getNextStepKey());
                }

                @Override
                protected Response newResponse(boolean acknowledged) {
                    return new Response(acknowledged);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
