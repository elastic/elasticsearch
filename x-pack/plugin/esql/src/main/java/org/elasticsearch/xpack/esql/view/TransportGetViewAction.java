/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

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
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportGetViewAction extends TransportLocalProjectMetadataAction<GetViewAction.Request, GetViewAction.Response> {

    private final ViewResolutionService viewResolutionService;

    @Inject
    public TransportGetViewAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(
            GetViewAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );
        this.viewResolutionService = new ViewResolutionService(indexNameExpressionResolver);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetViewAction.Request request,
        ProjectState project,
        ActionListener<GetViewAction.Response> listener
    ) {
        var result = viewResolutionService.resolveViews(
            project,
            request.indices(),
            request.indicesOptions(),
            request.getResolvedIndexExpressions()
        );
        listener.onResponse(new GetViewAction.Response(List.of(result.views())));
    }

    @Override
    protected ClusterBlockException checkBlock(GetViewAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
