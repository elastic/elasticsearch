/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPutDatasetAction extends AcknowledgedTransportMasterNodeProjectAction<PutDatasetAction.Request> {
    private final DatasetService datasetService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportPutDatasetAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DatasetService datasetService,
        ProjectResolver projectResolver
    ) {
        super(
            PutDatasetAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutDatasetAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.datasetService = datasetService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, PutDatasetAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        // Coord-side pre-check: parent lookup + validator dispatch against local (possibly stale)
        // cluster state. Fails fast without a master round-trip on unknown type, missing parent,
        // or validator rejection. The task body re-validates against master's authoritative state.
        try {
            var projectId = projectResolver.getProjectId();
            var project = clusterService.state().metadata().getProject(projectId);
            datasetService.validatePutDataset(project, request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(
        Task task,
        PutDatasetAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        datasetService.putDataset(state.projectId(), request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutDatasetAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(state.projectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
