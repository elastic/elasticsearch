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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Transport action that resolves the data stream names from the request and removes any configured data stream options from them.
 */
public class TransportDeleteDataStreamOptionsAction extends AcknowledgedTransportMasterNodeProjectAction<
    DeleteDataStreamOptionsAction.Request> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final MetadataDataStreamsService metadataDataStreamsService;
    private final SystemIndices systemIndices;

    @Inject
    public TransportDeleteDataStreamOptionsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MetadataDataStreamsService metadataDataStreamsService,
        SystemIndices systemIndices
    ) {
        super(
            DeleteDataStreamOptionsAction.INSTANCE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDataStreamOptionsAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.metadataDataStreamsService = metadataDataStreamsService;
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDataStreamOptionsAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        List<String> dataStreamNames = DataStreamsActionUtil.getDataStreamNames(
            indexNameExpressionResolver,
            state.metadata(),
            request.getNames(),
            request.indicesOptions()
        );
        for (String name : dataStreamNames) {
            systemIndices.validateDataStreamAccess(name, threadPool.getThreadContext());
        }
        metadataDataStreamsService.removeDataStreamOptions(
            state.projectId(),
            dataStreamNames,
            request.ackTimeout(),
            request.masterNodeTimeout(),
            listener
        );
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDataStreamOptionsAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
