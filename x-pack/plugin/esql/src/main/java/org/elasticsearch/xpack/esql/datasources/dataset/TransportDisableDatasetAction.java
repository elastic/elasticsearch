/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.List;

public class TransportDisableDatasetAction extends AcknowledgedTransportMasterNodeProjectAction<DisableDatasetAction.Request> {
    private final DatasetService datasetService;
    private final DatasetResolutionService datasetResolutionService;

    @Inject
    public TransportDisableDatasetAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DatasetService datasetService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DisableDatasetAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DisableDatasetAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.datasetService = datasetService;
        this.datasetResolutionService = new DatasetResolutionService(indexNameExpressionResolver);
    }

    @Override
    protected void masterOperation(
        Task task,
        DisableDatasetAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final DatasetResolutionService.DatasetResolutionResult result;
        try {
            result = datasetResolutionService.resolveDatasets(
                state,
                request.indices(),
                request.indicesOptions(),
                request.getResolvedIndexExpressions()
            );
        } catch (IndexNotFoundException e) {
            final String missing = e.getIndex() != null ? e.getIndex().getName() : String.join(",", request.names());
            listener.onFailure(new ResourceNotFoundException("dataset [{}] not found", missing));
            return;
        }
        if (result.datasets().length == 0) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }
        final List<String> datasetNames = Arrays.stream(result.datasets()).map(Dataset::name).toList();
        datasetService.setDatasetsEnabled(
            state.projectId(),
            request.masterNodeTimeout(),
            request.ackTimeout(),
            datasetNames,
            false,
            listener
        );
    }

    @Override
    protected ClusterBlockException checkBlock(DisableDatasetAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(state.projectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
