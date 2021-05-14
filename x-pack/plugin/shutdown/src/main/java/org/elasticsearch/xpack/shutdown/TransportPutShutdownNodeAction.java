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
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Objects;

public class TransportPutShutdownNodeAction extends AcknowledgedTransportMasterNodeAction<PutShutdownNodeAction.Request> {
    @Inject
    public TransportPutShutdownNodeAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PutShutdownNodeAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutShutdownNodeAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        PutShutdownNodeAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        clusterService.submitStateUpdateTask(
            "put-node-shutdown-" + request.getNodeId(),
            new AckedClusterStateUpdateTask(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    NodesShutdownMetadata currentShutdownMetadata = currentState.metadata().custom(NodesShutdownMetadata.TYPE);
                    if (currentShutdownMetadata == null) {
                        currentShutdownMetadata = new NodesShutdownMetadata(new HashMap<>());
                    }

                    // Verify that there's not already a shutdown metadata for this node
                    if (Objects.nonNull(currentShutdownMetadata.getAllNodeMetadataMap().get(request.getNodeId()))) {
                        throw new IllegalArgumentException("node [" + request.getNodeId() + "] is already shutting down");
                    }

                    SingleNodeShutdownMetadata newNodeMetadata = SingleNodeShutdownMetadata.builder()
                        .setNodeId(request.getNodeId())
                        .setType(request.getType())
                        .setReason(request.getReason())
                        .setStartedAtMillis(System.currentTimeMillis())
                        .build();

                    return ClusterState.builder(currentState)
                        .metadata(
                            Metadata.builder(currentState.metadata())
                                .putCustom(NodesShutdownMetadata.TYPE, currentShutdownMetadata.putSingleNodeMetadata(newNodeMetadata))
                        )
                        .build();
                }
            }
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PutShutdownNodeAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
