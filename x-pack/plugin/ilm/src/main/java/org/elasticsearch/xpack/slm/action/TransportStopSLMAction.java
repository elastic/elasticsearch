/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.action.StopSLMAction;
import org.elasticsearch.xpack.ilm.OperationModeUpdateTask;

import java.io.IOException;

public class TransportStopSLMAction extends TransportMasterNodeAction<StopSLMAction.Request, AcknowledgedResponse> {

    @Inject
    public TransportStopSLMAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(StopSLMAction.NAME, transportService, clusterService, threadPool, actionFilters, StopSLMAction.Request::new,
            indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(Task task, StopSLMAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("slm_operation_mode_update",
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return (OperationModeUpdateTask.slmMode(OperationMode.STOPPING)).execute(currentState);
                }

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(StopSLMAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
