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
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.action.RetryAction;
import org.elasticsearch.xpack.core.ilm.action.RetryAction.Request;
import org.elasticsearch.xpack.ilm.IndexLifecycleService;

public class TransportRetryAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRetryAction.class);

    IndexLifecycleService indexLifecycleService;

    @Inject
    public TransportRetryAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexLifecycleService indexLifecycleService
    ) {
        super(
            RetryAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            AcknowledgedResponse::readFrom,
            ThreadPool.Names.SAME
        );
        this.indexLifecycleService = indexLifecycleService;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        submitUnbatchedTask("ilm-re-run", new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return indexLifecycleService.moveClusterStateToPreviouslyFailedStep(currentState, request.indices());
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                for (String index : request.indices()) {
                    IndexMetadata idxMeta = newState.metadata().index(index);
                    LifecycleExecutionState lifecycleState = idxMeta.getLifecycleExecutionState();
                    StepKey retryStep = new StepKey(lifecycleState.phase(), lifecycleState.action(), lifecycleState.step());
                    if (idxMeta == null) {
                        // The index has somehow been deleted - there shouldn't be any opportunity for this to happen, but just in case.
                        logger.debug(
                            "index ["
                                + index
                                + "] has been deleted after moving to step ["
                                + lifecycleState.step()
                                + "], skipping async action check"
                        );
                        return;
                    }
                    indexLifecycleService.maybeRunAsyncAction(newState, idxMeta, retryStep);
                }
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
