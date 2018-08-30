/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportPutAutoFollowPatternAction extends
    TransportMasterNodeAction<PutAutoFollowPatternAction.Request, AcknowledgedResponse> {

    private final Client client;

    @Inject
    public TransportPutAutoFollowPatternAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                               ThreadPool threadPool, ActionFilters actionFilters, Client client,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PutAutoFollowPatternAction.NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, PutAutoFollowPatternAction.Request::new);
        this.client = client;
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
    protected void masterOperation(PutAutoFollowPatternAction.Request request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        final Client leaderClient;
        if (request.getLeaderClusterAlias().equals("_local_")) {
            leaderClient = client;
        } else {
            leaderClient = client.getRemoteClusterClient(request.getLeaderClusterAlias());
        }

        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metaData(true);

        leaderClient.admin().cluster().state(clusterStateRequest, ActionListener.wrap(clusterStateResponse -> {
            final ClusterState leaderClusterState = clusterStateResponse.getState();
            clusterService.submitStateUpdateTask("put-auto-follow-pattern-" + request.getLeaderClusterAlias(),
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        return innerPut(request, currentState, leaderClusterState);
                    }
                });
        }, listener::onFailure));
    }

    static ClusterState innerPut(PutAutoFollowPatternAction.Request request,
                                 ClusterState localState,
                                 ClusterState leaderClusterState) {
        // auto patterns are always overwritten
        // only already followed index uuids are updated

        AutoFollowMetadata currentAutoFollowMetadata = localState.metaData().custom(AutoFollowMetadata.TYPE);
        Map<String, List<String>> followedLeaderIndices;
        Map<String, AutoFollowMetadata.AutoFollowPattern> patterns;
        if (currentAutoFollowMetadata != null) {
            patterns = new HashMap<>(currentAutoFollowMetadata.getPatterns());
            followedLeaderIndices = new HashMap<>(currentAutoFollowMetadata.getFollowedLeaderIndexUUIDs());
        } else {
            patterns = new HashMap<>();
            followedLeaderIndices = new HashMap<>();
        }

        AutoFollowMetadata.AutoFollowPattern previousPattern = patterns.get(request.getLeaderClusterAlias());
        List<String> followedIndexUUIDs = followedLeaderIndices.get(request.getLeaderClusterAlias());
        if (followedIndexUUIDs == null) {
            followedIndexUUIDs = new ArrayList<>();
            followedLeaderIndices.put(request.getLeaderClusterAlias(), followedIndexUUIDs);
        }

        // Mark existing leader indices as already auto followed:
        if (previousPattern != null) {
            for (String newPattern : request.getLeaderIndexPatterns()) {
                if (previousPattern.getLeaderIndexPatterns().contains(newPattern) == false) {
                    for (IndexMetaData indexMetaData : leaderClusterState.getMetaData()) {
                        if (Regex.simpleMatch(newPattern, indexMetaData.getIndex().getName())) {
                            followedIndexUUIDs.add(indexMetaData.getIndexUUID());
                        }
                    }
                }
            }
        } else {
            for (IndexMetaData indexMetaData : leaderClusterState.getMetaData()) {
                String[] indexPatterns = request.getLeaderIndexPatterns().toArray(new String[0]);
                if (Regex.simpleMatch(indexPatterns, indexMetaData.getIndex().getName())) {
                    followedIndexUUIDs.add(indexMetaData.getIndexUUID());
                }
            }
        }

        AutoFollowMetadata.AutoFollowPattern autoFollowPattern = new AutoFollowMetadata.AutoFollowPattern(request.getLeaderIndexPatterns(),
            request.getFollowIndexNamePattern(), request.getMaxBatchOperationCount(), request.getMaxConcurrentReadBatches(),
            request.getMaxOperationSizeInBytes(), request.getMaxConcurrentWriteBatches(), request.getMaxWriteBufferSize(),
            request.getRetryTimeout(), request.getIdleShardRetryDelay());
        patterns.put(request.getLeaderClusterAlias(), autoFollowPattern);
        ClusterState.Builder newState = ClusterState.builder(localState);
        newState.metaData(MetaData.builder(localState.getMetaData())
            .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, followedLeaderIndices))
            .build());
        return newState.build();
    }

    @Override
    protected ClusterBlockException checkBlock(PutAutoFollowPatternAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
