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
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportDeleteDatasetAction extends AcknowledgedTransportMasterNodeProjectAction<DeleteDatasetAction.Request> {
    private final DatasetService datasetService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportDeleteDatasetAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DatasetService datasetService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            DeleteDatasetAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDatasetAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.datasetService = datasetService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected void doExecute(Task task, DeleteDatasetAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        destructiveOperations.failDestructive(request.names());
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDatasetAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // Resolve to datasets only: `resolveDatasets` is additive, so a wildcard expands across the whole
        // namespace — without this filter index names reach the registry. Mirrors TransportDeleteViewAction.
        final List<String> datasetNames;
        try {
            datasetNames = indexNameExpressionResolver.datasets(state.metadata(), request.indicesOptions(), request);
        } catch (IndexNotFoundException e) {
            final String missing = e.getIndex() != null ? e.getIndex().getName() : String.join(",", request.names());
            listener.onFailure(new ResourceNotFoundException("dataset [{}] not found", missing));
            return;
        }
        if (datasetNames.isEmpty()) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }
        datasetService.deleteDatasets(state.projectId(), request.masterNodeTimeout(), request.ackTimeout(), datasetNames, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDatasetAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(state.projectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
