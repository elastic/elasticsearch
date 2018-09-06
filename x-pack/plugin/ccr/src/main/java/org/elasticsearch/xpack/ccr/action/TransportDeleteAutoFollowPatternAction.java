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
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportDeleteAutoFollowPatternAction extends
    TransportMasterNodeAction<DeleteAutoFollowPatternAction.Request, AcknowledgedResponse> {

    @Inject
    public TransportDeleteAutoFollowPatternAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                                  ThreadPool threadPool, ActionFilters actionFilters,
                                                  IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, DeleteAutoFollowPatternAction.NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, DeleteAutoFollowPatternAction.Request::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(DeleteAutoFollowPatternAction.Request request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask("put-auto-follow-pattern-" + request.getLeaderClusterAlias(),
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return innerDelete(request, currentState);
            }
        });
    }

    static ClusterState innerDelete(DeleteAutoFollowPatternAction.Request request, ClusterState currentState) {
        AutoFollowMetadata currentAutoFollowMetadata = currentState.metaData().custom(AutoFollowMetadata.TYPE);
        if (currentAutoFollowMetadata == null) {
            throw new ResourceNotFoundException("no auto-follow patterns for cluster alias [{}] found",
                request.getLeaderClusterAlias());
        }
        Map<String, AutoFollowPattern> patterns = currentAutoFollowMetadata.getPatterns();
        AutoFollowPattern autoFollowPatternToRemove = patterns.get(request.getLeaderClusterAlias());
        if (autoFollowPatternToRemove == null) {
            throw new ResourceNotFoundException("no auto-follow patterns for cluster alias [{}] found",
                request.getLeaderClusterAlias());
        }

        final Map<String, AutoFollowPattern> patternsCopy = new HashMap<>(patterns);
        final Map<String, List<String>> followedLeaderIndexUUIDSCopy =
            new HashMap<>(currentAutoFollowMetadata.getFollowedLeaderIndexUUIDs());
        patternsCopy.remove(request.getLeaderClusterAlias());
        followedLeaderIndexUUIDSCopy.remove(request.getLeaderClusterAlias());

        AutoFollowMetadata newAutoFollowMetadata = new AutoFollowMetadata(patternsCopy, followedLeaderIndexUUIDSCopy);
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData())
            .putCustom(AutoFollowMetadata.TYPE, newAutoFollowMetadata)
            .build());
        return newState.build();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteAutoFollowPatternAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
