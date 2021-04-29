/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.action.MoveToStepAction;
import org.elasticsearch.xpack.core.ilm.action.MoveToStepAction.Request;
import org.elasticsearch.xpack.ilm.IndexLifecycleService;

public class TransportMoveToStepAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(TransportMoveToStepAction.class);

    IndexLifecycleService indexLifecycleService;
    @Inject
    public TransportMoveToStepAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                     IndexLifecycleService indexLifecycleService) {
        super(MoveToStepAction.NAME, transportService, clusterService, threadPool, actionFilters, Request::new,
            indexNameExpressionResolver, AcknowledgedResponse::readFrom, ThreadPool.Names.SAME);
        this.indexLifecycleService = indexLifecycleService;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        IndexMetadata indexMetadata = state.metadata().index(request.getIndex());
        if (indexMetadata == null) {
            listener.onFailure(new IllegalArgumentException("index [" + request.getIndex() + "] does not exist"));
            return;
        }
        clusterService.submitStateUpdateTask("index[" + request.getIndex() + "]-move-to-step",
            new AckedClusterStateUpdateTask(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return indexLifecycleService.moveClusterStateToStep(currentState, indexMetadata.getIndex(), request.getCurrentStepKey(),
                        request.getNextStepKey());
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    IndexMetadata newIndexMetadata = newState.metadata().index(indexMetadata.getIndex());
                    if (newIndexMetadata == null) {
                        // The index has somehow been deleted - there shouldn't be any opportunity for this to happen, but just in case.
                        logger.debug("index [" + indexMetadata.getIndex() + "] has been deleted after moving to step [" +
                            request.getNextStepKey() + "], skipping async action check");
                        return;
                    }
                    indexLifecycleService.maybeRunAsyncAction(newState, newIndexMetadata, request.getNextStepKey());
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
