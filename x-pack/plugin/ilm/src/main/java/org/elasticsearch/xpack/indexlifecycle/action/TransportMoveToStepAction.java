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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.indexlifecycle.MoveToStepRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.MoveToStepResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepAction;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycleService;

public class TransportMoveToStepAction extends TransportMasterNodeAction<MoveToStepRequest, MoveToStepResponse> {
    IndexLifecycleService indexLifecycleService;
    @Inject
    public TransportMoveToStepAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver,
                                     IndexLifecycleService indexLifecycleService) {
        super(settings, MoveToStepAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                MoveToStepRequest::new);
        this.indexLifecycleService = indexLifecycleService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected MoveToStepResponse newResponse() {
        return new MoveToStepResponse();
    }

    @Override
    protected void masterOperation(MoveToStepRequest request, ClusterState state, ActionListener<MoveToStepResponse> listener) {
        IndexMetaData indexMetaData = state.metaData().index(request.getIndex());
        if (indexMetaData == null) {
            listener.onFailure(new IllegalArgumentException("index [" + request.getIndex() + "] does not exist"));
            return;
        }
        clusterService.submitStateUpdateTask("index[" + request.getIndex() + "]-move-to-step",
            new AckedClusterStateUpdateTask<MoveToStepResponse>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return indexLifecycleService.moveClusterStateToStep(currentState, request.getIndex(), request.getCurrentStepKey(),
                        request.getNextStepKey());
                }

                @Override
                protected MoveToStepResponse newResponse(boolean acknowledged) {
                    return new MoveToStepResponse(acknowledged);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(MoveToStepRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
