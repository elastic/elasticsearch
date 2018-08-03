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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.indexlifecycle.PutOperationModeRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.PutOperationModeResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutOperationModeAction;
import org.elasticsearch.xpack.indexlifecycle.OperationModeUpdateTask;

public class TransportPutOperationModeAction extends TransportMasterNodeAction<PutOperationModeRequest, PutOperationModeResponse> {

    @Inject
    public TransportPutOperationModeAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           ThreadPool threadPool, ActionFilters actionFilters,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PutOperationModeAction.NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, PutOperationModeRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutOperationModeResponse newResponse() {
        return new PutOperationModeResponse();
    }

    @Override
    protected void masterOperation(PutOperationModeRequest request, ClusterState state, ActionListener<PutOperationModeResponse> listener) {
        clusterService.submitStateUpdateTask("ilm_operation_mode_update",
            new AckedClusterStateUpdateTask<PutOperationModeResponse>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return (new OperationModeUpdateTask(request.getMode())).execute(currentState);
                }

                @Override
                protected PutOperationModeResponse newResponse(boolean acknowledged) {
                    return new PutOperationModeResponse(acknowledged);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(PutOperationModeRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
