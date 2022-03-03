/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteDesiredNodesAction extends TransportMasterNodeAction<DeleteDesiredNodesAction.Request, ActionResponse.Empty> {
    private final DesiredNodesClusterStateTaskExecutor taskExecutor;

    @Inject
    public TransportDeleteDesiredNodesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DeleteDesiredNodesAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDesiredNodesAction.Request::new,
            indexNameExpressionResolver,
            in -> ActionResponse.Empty.INSTANCE,
            ThreadPool.Names.SAME
        );
        this.taskExecutor = new DesiredNodesClusterStateTaskExecutor();
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDesiredNodesAction.Request request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) throws Exception {
        clusterService.submitStateUpdateTask("delete-desired-nodes", new ClusterStateUpdateTask(Priority.HIGH) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState.copyAndUpdateMetadata(metadata -> metadata.removeCustom(DesiredNodesMetadata.TYPE));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(ActionResponse.Empty.INSTANCE);
            }
        }, taskExecutor);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDesiredNodesAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
