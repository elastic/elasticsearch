/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.options.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.DataStreamsActionUtil;
import org.elasticsearch.action.datastreams.PutDataStreamOptionsAction;
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
 * Transport action that resolves the data stream names from the request and sets the data stream lifecycle provided in the request.
 */
public class TransportPutDataStreamOptionsAction extends AcknowledgedTransportMasterNodeAction<PutDataStreamOptionsAction.Request> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final MetadataDataStreamsService metadataDataStreamsService;
    private final SystemIndices systemIndices;

    @Inject
    public TransportPutDataStreamOptionsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MetadataDataStreamsService metadataDataStreamsService,
        SystemIndices systemIndices
    ) {
        super(
            PutDataStreamOptionsAction.INSTANCE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutDataStreamOptionsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.metadataDataStreamsService = metadataDataStreamsService;
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutDataStreamOptionsAction.Request request,
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
        metadataDataStreamsService.setDataStreamOptions(
            dataStreamNames,
            request.getOptions(),
            request.ackTimeout(),
            request.masterNodeTimeout(),
            listener
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataStreamOptionsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
