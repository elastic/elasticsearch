/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.DataStreamsActionUtil;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Transport action that resolves the data stream names from the request and removes any configured data stream lifecycle from them.
 */
public class TransportDeleteDataStreamLifecycleAction extends AcknowledgedTransportMasterNodeAction<
    DeleteDataStreamLifecycleAction.Request> {

    private final MetadataDataStreamsService metadataDataStreamsService;
    private final SystemIndices systemIndices;

    @Inject
    public TransportDeleteDataStreamLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MetadataDataStreamsService metadataDataStreamsService,
        SystemIndices systemIndices
    ) {
        super(
            DeleteDataStreamLifecycleAction.INSTANCE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDataStreamLifecycleAction.Request::new,
            indexNameExpressionResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.metadataDataStreamsService = metadataDataStreamsService;
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDataStreamLifecycleAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        List<String> dataStreamNames = DataStreamsActionUtil.getDataStreamNames(
            indexNameExpressionResolver,
            state,
            request.getNames(),
            request.indicesOptions()
        );
        for (String name : dataStreamNames) {
            systemIndices.validateDataStreamAccess(name, threadPool.getThreadContext());
        }
        metadataDataStreamsService.removeLifecycle(dataStreamNames, request.ackTimeout(), request.masterNodeTimeout(), listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDataStreamLifecycleAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
