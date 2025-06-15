/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.RetryActionRequest;
import org.elasticsearch.xpack.ilm.IndexLifecycleService;

public class TransportRetryAction extends TransportMasterNodeAction<RetryActionRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRetryAction.class);

    private final IndexLifecycleService indexLifecycleService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportRetryAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexLifecycleService indexLifecycleService,
        ProjectResolver projectResolver
    ) {
        super(
            ILMActions.RETRY.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RetryActionRequest::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexLifecycleService = indexLifecycleService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        RetryActionRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final var projectState = projectResolver.getProjectState(state);
        if (request.requireError() == false) {
            maybeRunAsyncAction(projectState, request.indices());
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }
        submitUnbatchedTask("ilm-re-run", new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final var project = state.metadata().getProject(projectState.projectId());
                final var updatedProject = indexLifecycleService.moveIndicesToPreviouslyFailedStep(project, request.indices());
                return ClusterState.builder(currentState).putProjectMetadata(updatedProject).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                maybeRunAsyncAction(newState.projectState(projectState.projectId()), request.indices());
            }
        });
    }

    private void maybeRunAsyncAction(ProjectState state, String[] indices) {
        for (String index : indices) {
            IndexMetadata idxMeta = state.metadata().index(index);
            if (idxMeta == null) {
                // The index has somehow been deleted - there shouldn't be any opportunity for this to happen, but just in case.
                logger.debug("index [" + index + "] has been deleted, skipping async action check");
                return;
            }
            LifecycleExecutionState lifecycleState = idxMeta.getLifecycleExecutionState();
            StepKey retryStep = new StepKey(lifecycleState.phase(), lifecycleState.action(), lifecycleState.step());
            indexLifecycleService.maybeRunAsyncAction(state.cluster(), idxMeta, retryStep);
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected ClusterBlockException checkBlock(RetryActionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
