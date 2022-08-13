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
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
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

    private final ClusterStateTaskExecutor<DeleteDesiredNodesTask> taskExecutor = new DeleteDesiredNodesExecutor();

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
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDesiredNodesAction.Request request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) throws Exception {
        clusterService.submitStateUpdateTask(
            "delete-desired-nodes",
            new DeleteDesiredNodesTask(listener),
            ClusterStateTaskConfig.build(Priority.HIGH, request.masterNodeTimeout()),
            taskExecutor
        );
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDesiredNodesAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private record DeleteDesiredNodesTask(ActionListener<ActionResponse.Empty> listener) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private static class DeleteDesiredNodesExecutor implements ClusterStateTaskExecutor<DeleteDesiredNodesTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<DeleteDesiredNodesTask> batchExecutionContext) throws Exception {
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                taskContext.success(() -> taskContext.getTask().listener().onResponse(ActionResponse.Empty.INSTANCE));
            }
            return batchExecutionContext.initialState().copyAndUpdateMetadata(metadata -> metadata.removeCustom(DesiredNodesMetadata.TYPE));
        }
    }
}
