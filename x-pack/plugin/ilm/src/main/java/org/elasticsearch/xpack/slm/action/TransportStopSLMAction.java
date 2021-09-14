/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.action.StopSLMAction;
import org.elasticsearch.xpack.ilm.OperationModeUpdateTask;

public class TransportStopSLMAction extends AcknowledgedTransportMasterNodeAction<StopSLMAction.Request> {

    @Inject
    public TransportStopSLMAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(StopSLMAction.NAME, transportService, clusterService, threadPool, actionFilters, StopSLMAction.Request::new,
            indexNameExpressionResolver, ThreadPool.Names.SAME);
    }

    @Override
    protected void masterOperation(Task task, StopSLMAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("slm_operation_mode_update", new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return (OperationModeUpdateTask.slmMode(OperationMode.STOPPING)).execute(currentState);
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(StopSLMAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
