/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/** Local transport handler for {@link GetDatasetAction}. */
public class TransportGetDatasetAction extends TransportLocalProjectMetadataAction<GetDatasetAction.Request, GetDatasetAction.Response> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

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
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetDatasetAction.Request request,
        ProjectState project,
        ActionListener<GetDatasetAction.Response> listener
    ) {
        final DatasetMetadata metadata = DatasetMetadata.get(project.metadata());
        final List<String> resolved = indexNameExpressionResolver.datasets(project.metadata(), request.indicesOptions(), request);
        final List<Dataset> hits = new ArrayList<>();
        for (String name : resolved) {
            Dataset ds = metadata.get(name);
            if (ds != null) {
                hits.add(ds);
            }
        }
        listener.onResponse(new GetDatasetAction.Response(hits));
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatasetAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
