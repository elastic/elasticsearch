/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction.Request;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TransportActivateAutoFollowPatternAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    @Inject
    public TransportActivateAutoFollowPatternAction(TransportService transportService, ClusterService clusterService,
                                                    ThreadPool threadPool, ActionFilters actionFilters,
                                                    IndexNameExpressionResolver resolver) {
        super(ActivateAutoFollowPatternAction.NAME, transportService, clusterService, threadPool, actionFilters, Request::new, resolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(final StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(final Task task, final Request request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("activate-auto-follow-pattern-" + request.getName(),
            new AckedClusterStateUpdateTask<>(request, listener) {

                @Override
                protected AcknowledgedResponse newResponse(final boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }

                @Override
                public ClusterState execute(final ClusterState currentState) throws Exception {
                    return innerActivate(request, currentState);
                }
            });
    }

    static ClusterState innerActivate(final Request request, ClusterState currentState) {
        final AutoFollowMetadata autoFollowMetadata = currentState.metadata().custom(AutoFollowMetadata.TYPE);
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
        newPatterns.put(request.getName(),
            new AutoFollowMetadata.AutoFollowPattern(
                previousAutoFollowPattern.getRemoteCluster(),
                previousAutoFollowPattern.getLeaderIndexPatterns(),
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
                previousAutoFollowPattern.getReadPollTimeout()));

        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.getMetadata())
                .putCustom(AutoFollowMetadata.TYPE,
                    new AutoFollowMetadata(newPatterns, autoFollowMetadata.getFollowedLeaderIndexUUIDs(), autoFollowMetadata.getHeaders()))
                .build())
            .build();
    }
}
