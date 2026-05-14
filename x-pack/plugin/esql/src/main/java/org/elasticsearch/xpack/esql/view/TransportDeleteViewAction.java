/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

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

public class TransportDeleteViewAction extends AcknowledgedTransportMasterNodeProjectAction<DeleteViewAction.Request> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final DestructiveOperations destructiveOperations;
    private final ViewService viewService;

    @Inject
    public TransportDeleteViewAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        ViewService viewService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            DeleteViewAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteViewAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.viewService = viewService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.destructiveOperations = destructiveOperations;
    }

    @Override
    protected void doExecute(Task task, DeleteViewAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        destructiveOperations.failDestructive(request.views());
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteViewAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        List<String> viewNames;
        try {
            viewNames = indexNameExpressionResolver.views(state.metadata(), request.indicesOptions(), request);
        } catch (IndexNotFoundException e) {
            listener.onFailure(new ResourceNotFoundException("view [{}] not found", e.getIndex().getName()));
            return;
        }
        if (viewNames.isEmpty()) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }
        viewService.deleteViews(state.projectId(), request.masterNodeTimeout(), request.ackTimeout(), viewNames, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteViewAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
