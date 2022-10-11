/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.PromoteDataStreamAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class PromoteDataStreamTransportAction extends AcknowledgedTransportMasterNodeAction<PromoteDataStreamAction.Request> {

    private final SystemIndices systemIndices;

    @Inject
    public PromoteDataStreamTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            PromoteDataStreamAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PromoteDataStreamAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(
        Task task,
        PromoteDataStreamAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        systemIndices.validateDataStreamAccess(request.getName(), threadPool.getThreadContext());
        submitUnbatchedTask(
            "promote-data-stream [" + request.getName() + "]",
            new ClusterStateUpdateTask(Priority.HIGH, request.masterNodeTimeout()) {

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return promoteDataStream(currentState, request);
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }
            }
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    static ClusterState promoteDataStream(ClusterState currentState, PromoteDataStreamAction.Request request) {
        DataStream dataStream = currentState.getMetadata().dataStreams().get(request.getName());
        if (dataStream == null) {
            throw new ResourceNotFoundException("data stream [" + request.getName() + "] does not exist");
        }

        DataStream promotedDataStream = dataStream.promoteDataStream();
        Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        metadata.put(promotedDataStream);
        return ClusterState.builder(currentState).metadata(metadata).build();
    }

    @Override
    protected ClusterBlockException checkBlock(PromoteDataStreamAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
