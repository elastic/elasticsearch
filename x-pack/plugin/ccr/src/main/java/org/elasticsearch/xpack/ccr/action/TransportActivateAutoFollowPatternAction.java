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
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction.Request;

import java.util.HashMap;
import java.util.Map;

public class TransportActivateAutoFollowPatternAction extends AcknowledgedTransportMasterNodeAction<Request> {

    @Inject
    public TransportActivateAutoFollowPatternAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            ActivateAutoFollowPatternAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        final Task task,
        final Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        submitUnbatchedTask("activate-auto-follow-pattern-" + request.getName(), new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(final ClusterState currentState) {
                return innerActivate(request, currentState);
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    static ClusterState innerActivate(final Request request, ClusterState currentState) {
        final AutoFollowMetadata autoFollowMetadata = currentState.metadata().getProject().custom(AutoFollowMetadata.TYPE);
        if (autoFollowMetadata == null) {
            throw new ResourceNotFoundException("auto-follow pattern [{}] is missing", request.getName());
        }

        final Map<String, AutoFollowMetadata.AutoFollowPattern> patterns = autoFollowMetadata.getPatterns();
        final AutoFollowMetadata.AutoFollowPattern previousAutoFollowPattern = patterns.get(request.getName());
        if (previousAutoFollowPattern == null) {
            throw new ResourceNotFoundException("auto-follow pattern [{}] is missing", request.getName());
        }

        if (previousAutoFollowPattern.isActive() == request.isActive()) {
            return currentState;
        }

        final Map<String, AutoFollowMetadata.AutoFollowPattern> newPatterns = new HashMap<>(patterns);
        newPatterns.put(
            request.getName(),
            new AutoFollowMetadata.AutoFollowPattern(
                previousAutoFollowPattern.getRemoteCluster(),
                previousAutoFollowPattern.getLeaderIndexPatterns(),
                previousAutoFollowPattern.getLeaderIndexExclusionPatterns(),
                previousAutoFollowPattern.getFollowIndexPattern(),
                previousAutoFollowPattern.getSettings(),
                request.isActive(),
                previousAutoFollowPattern.getMaxReadRequestOperationCount(),
                previousAutoFollowPattern.getMaxWriteRequestOperationCount(),
                previousAutoFollowPattern.getMaxOutstandingReadRequests(),
                previousAutoFollowPattern.getMaxOutstandingWriteRequests(),
                previousAutoFollowPattern.getMaxReadRequestSize(),
                previousAutoFollowPattern.getMaxWriteRequestSize(),
                previousAutoFollowPattern.getMaxWriteBufferCount(),
                previousAutoFollowPattern.getMaxWriteBufferSize(),
                previousAutoFollowPattern.getMaxRetryDelay(),
                previousAutoFollowPattern.getReadPollTimeout()
            )
        );

        return currentState.copyAndUpdateMetadata(
            metadata -> metadata.putCustom(
                AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(newPatterns, autoFollowMetadata.getFollowedLeaderIndexUUIDs(), autoFollowMetadata.getHeaders())
            )
        );
    }
}
