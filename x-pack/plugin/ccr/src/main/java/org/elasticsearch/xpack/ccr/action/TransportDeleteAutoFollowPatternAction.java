/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;

public class TransportDeleteAutoFollowPatternAction extends AcknowledgedTransportMasterNodeAction<DeleteAutoFollowPatternAction.Request> {

    private final ProjectResolver projectResolver;

    @Inject
    public TransportDeleteAutoFollowPatternAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            DeleteAutoFollowPatternAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteAutoFollowPatternAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteAutoFollowPatternAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        submitUnbatchedTask("delete-auto-follow-pattern-" + request.getName(), new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return innerDelete(request, projectResolver.getProjectState(currentState));
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    static ClusterState innerDelete(DeleteAutoFollowPatternAction.Request request, ProjectState projectState) {
        AutoFollowMetadata currentAutoFollowMetadata = projectState.metadata().custom(AutoFollowMetadata.TYPE);
        if (currentAutoFollowMetadata == null || currentAutoFollowMetadata.getPatterns().get(request.getName()) == null) {
            throw new ResourceNotFoundException("auto-follow pattern [{}] is missing", request.getName());
        }

        AutoFollowMetadata newAutoFollowMetadata = removePattern(currentAutoFollowMetadata, request.getName());

        return projectState.updatedState(metadata -> metadata.putCustom(AutoFollowMetadata.TYPE, newAutoFollowMetadata));
    }

    private static AutoFollowMetadata removePattern(AutoFollowMetadata metadata, String name) {
        return new AutoFollowMetadata(
            Maps.copyMapWithRemovedEntry(metadata.getPatterns(), name),
            Maps.copyMapWithRemovedEntry(metadata.getFollowedLeaderIndexUUIDs(), name),
            Maps.copyMapWithRemovedEntry(metadata.getHeaders(), name)
        );
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteAutoFollowPatternAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
