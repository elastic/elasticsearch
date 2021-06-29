/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.datastreams.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;

public class CreateDataStreamTransportAction extends AcknowledgedTransportMasterNodeAction<CreateDataStreamAction.Request> {

    private final MetadataCreateDataStreamService metadataCreateDataStreamService;
    private final SystemIndices systemIndices;

    @Inject
    public CreateDataStreamTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MetadataCreateDataStreamService metadataCreateDataStreamService,
        SystemIndices systemIndices
    ) {
        super(
            CreateDataStreamAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CreateDataStreamAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.metadataCreateDataStreamService = metadataCreateDataStreamService;
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(
        Task task,
        CreateDataStreamAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        final SystemDataStreamDescriptor systemDataStreamDescriptor = systemIndices.validateDataStreamAccess(
            request.getName(),
            threadPool.getThreadContext()
        );
        MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest updateRequest =
            new MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest(
                request.getName(),
                systemDataStreamDescriptor,
                request.masterNodeTimeout(),
                request.timeout()
            );
        metadataCreateDataStreamService.createDataStream(updateRequest, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateDataStreamAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
