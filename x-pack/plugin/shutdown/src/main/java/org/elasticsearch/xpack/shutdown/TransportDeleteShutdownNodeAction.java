/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteShutdownNodeAction extends AcknowledgedTransportMasterNodeAction<DeleteShutdownNodeAction.Request> {
    @Inject
    public TransportDeleteShutdownNodeAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DeleteShutdownNodeAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteShutdownNodeAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteShutdownNodeAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        { // This block solely to ensure this NodesShutdownMetadata isn't accidentally used in the cluster state update task below
            NodesShutdownMetadata nodesShutdownMetadata = state.metadata().custom(NodesShutdownMetadata.TYPE);
            if (nodesShutdownMetadata.getAllNodeMetadataMap().get(request.getNodeId()) == null) {
                throw new IllegalArgumentException("node [" + request.getNodeId() + "] is not currently shutting down");
            }
        }

        clusterService.submitStateUpdateTask(
            "delete-node-shutdown-" + request.getNodeId(),
            new AckedClusterStateUpdateTask(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    NodesShutdownMetadata currentShutdownMetadata = currentState.metadata().custom(NodesShutdownMetadata.TYPE);

                    return ClusterState.builder(currentState)
                        .metadata(
                            Metadata.builder(currentState.metadata())
                                .putCustom(
                                    NodesShutdownMetadata.TYPE,
                                    currentShutdownMetadata.removeSingleNodeMetadata(request.getNodeId())
                                )
                        )
                        .build();

                }
            }
        );
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteShutdownNodeAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
