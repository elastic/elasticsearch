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
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/** Local transport handler for {@link GetDatasetAction}. */
public class TransportGetDatasetAction extends TransportLocalProjectMetadataAction<GetDatasetAction.Request, GetDatasetAction.Response> {

    private final DatasetResolutionService datasetResolutionService;

    @Inject
    public TransportGetDatasetAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(
            GetDatasetAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );
        this.datasetResolutionService = new DatasetResolutionService(indexNameExpressionResolver);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetDatasetAction.Request request,
        ProjectState project,
        ActionListener<GetDatasetAction.Response> listener
    ) {
        // An explicit name that doesn't exist, isn't visible, or exists only as a co-resident foreign resource
        // (e.g. a data stream) throws IndexNotFoundException. Translate it to a dataset-shaped not-found instead
        // of leaking a raw index_not_found_exception, mirroring TransportDeleteDatasetAction.
        final DatasetResolutionService.DatasetResolutionResult result;
        try {
            result = datasetResolutionService.resolveDatasets(
                project,
                request.indices(),
                request.indicesOptions(),
                request.getResolvedIndexExpressions()
            );
        } catch (IndexNotFoundException e) {
            final String missing = e.getIndex() != null ? e.getIndex().getName() : String.join(",", request.indices());
            listener.onFailure(new ResourceNotFoundException("dataset [{}] not found", missing));
            return;
        }
        listener.onResponse(new GetDatasetAction.Response(List.of(result.datasets())));
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatasetAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
