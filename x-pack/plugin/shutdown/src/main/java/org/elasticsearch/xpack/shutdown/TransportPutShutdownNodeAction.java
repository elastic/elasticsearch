/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;

public class TransportPutShutdownNodeAction extends AcknowledgedTransportMasterNodeAction<PutShutdownNodeAction.Request> {
    private static final Logger logger = LogManager.getLogger(TransportPutShutdownNodeAction.class);

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
        clusterService.submitStateUpdateTask("put-node-shutdown-" + request.getNodeId(), new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                NodesShutdownMetadata currentShutdownMetadata = currentState.metadata().custom(NodesShutdownMetadata.TYPE);
                if (currentShutdownMetadata == null) {
                    currentShutdownMetadata = new NodesShutdownMetadata(new HashMap<>());
                }

                // Verify that there's not already a shutdown metadata for this node
                SingleNodeShutdownMetadata existingRecord = currentShutdownMetadata.getAllNodeMetadataMap().get(request.getNodeId());
                if (existingRecord != null) {
                    logger.info(
                        "updating existing shutdown record for node [{}] of type [{}] with reason [{}] with new type [{}] and reason [{}]",
                        existingRecord.getNodeId(),
                        existingRecord.getType(),
                        existingRecord.getReason(),
                        request.getType(),
                        request.getReason()
                    );
                }

                final boolean nodeSeen = currentState.getNodes().nodeExists(request.getNodeId());

                SingleNodeShutdownMetadata newNodeMetadata = SingleNodeShutdownMetadata.builder()
                    .setNodeId(request.getNodeId())
                    .setType(request.getType())
                    .setReason(request.getReason())
                    .setStartedAtMillis(System.currentTimeMillis())
                    .setNodeSeen(nodeSeen)
                    .build();

                return ClusterState.builder(currentState)
                    .metadata(
                        Metadata.builder(currentState.metadata())
                            .putCustom(NodesShutdownMetadata.TYPE, currentShutdownMetadata.putSingleNodeMetadata(newNodeMetadata))
                    )
                    .build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error(new ParameterizedMessage("failed to put shutdown for node [{}]", request.getNodeId()), e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (SingleNodeShutdownMetadata.Type.REMOVE.equals(request.getType())) {
                    clusterService.getRerouteService()
                        .reroute("node registered for removal from cluster", Priority.NORMAL, new ActionListener<ClusterState>() {
                            @Override
                            public void onResponse(ClusterState clusterState) {
                                logger.trace("started reroute after registering node [{}] for removal", request.getNodeId());
                                listener.onResponse(AcknowledgedResponse.TRUE);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn(
                                    new ParameterizedMessage(
                                        "failed to start reroute after registering node [{}] for removal",
                                        request.getNodeId()
                                    ),
                                    e
                                );
                                listener.onFailure(e);
                            }
                        });
                } else {
                    logger.trace(
                        "not starting reroute after registering node ["
                            + request.getNodeId()
                            + "] for shutdown of type ["
                            + request.getType()
                            + "]"
                    );
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(PutShutdownNodeAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
