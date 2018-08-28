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
        final Client remoteClient;
        if (request.getRemoteClusterAlias().equals("_local_")) {
            remoteClient = client;
        } else {
            remoteClient = client.getRemoteClusterClient(request.getRemoteClusterAlias());
        }

        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metaData(true);

        remoteClient.admin().cluster().state(clusterStateRequest, ActionListener.wrap(resp -> {
            clusterService.submitStateUpdateTask("put_auto_follow_pattern-" + request.getRemoteClusterAlias(),
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        return innerPut(request, currentState, resp.getState());
                    }
                });
        }, listener::onFailure));
    }

    static ClusterState innerPut(PutAutoFollowPatternAction.Request request, ClusterState localState, ClusterState remoteClusterState) {
        // auto patterns are always overwritten
        // only already followed index uuids are updated

        AutoFollowMetadata currentAutoFollowMetadata = localState.metaData().custom(AutoFollowMetadata.TYPE);
        Map<String, List<String>> followedLeaderIndices;
        Map<String, AutoFollowMetadata.AutoFollowPattern> configurations;
        if (currentAutoFollowMetadata != null) {
            configurations = new HashMap<>(currentAutoFollowMetadata.getPatterns());
            followedLeaderIndices = new HashMap<>(currentAutoFollowMetadata.getFollowedLeaderIndexUUIDs());
        } else {
            configurations = new HashMap<>();
            followedLeaderIndices = new HashMap<>();
        }

        AutoFollowMetadata.AutoFollowPattern previousPattern = configurations.get(request.getRemoteClusterAlias());
        List<String> followedIndexUUIDS = followedLeaderIndices.get(request.getRemoteClusterAlias());
        if (followedIndexUUIDS == null) {
            followedIndexUUIDS = new ArrayList<>();
            followedLeaderIndices.put(request.getRemoteClusterAlias(), followedIndexUUIDS);
        }

        // Mark existing leader indices as already auto followed:
        if (previousPattern != null) {
            for (String newPattern : request.getLeaderIndexPatterns()) {
                if (previousPattern.getLeaderIndexPatterns().contains(newPattern) == false) {
                    for (IndexMetaData indexMetaData : remoteClusterState.getMetaData()) {
                        if (Regex.simpleMatch(newPattern, indexMetaData.getIndex().getName())) {
                            followedIndexUUIDS.add(indexMetaData.getIndexUUID());
                        }
                    }
                }
            }
        } else {
            for (IndexMetaData indexMetaData : remoteClusterState.getMetaData()) {
                String[] patterns = request.getLeaderIndexPatterns().toArray(new String[0]);
                if (Regex.simpleMatch(patterns, indexMetaData.getIndex().getName())) {
                    followedIndexUUIDS.add(indexMetaData.getIndexUUID());
                }
            }
        }

        AutoFollowMetadata.AutoFollowPattern autoFollowPattern = new AutoFollowMetadata.AutoFollowPattern(request.getLeaderIndexPatterns(),
            request.getFollowIndexNamePattern(), request.getMaxBatchOperationCount(), request.getMaxConcurrentReadBatches(),
            request.getMaxOperationSizeInBytes(), request.getMaxConcurrentWriteBatches(), request.getMaxWriteBufferSize(),
            request.getRetryTimeout(), request.getIdleShardRetryDelay());
        configurations.put(request.getRemoteClusterAlias(), autoFollowPattern);
        ClusterState.Builder newState = ClusterState.builder(localState);
        newState.metaData(MetaData.builder(localState.getMetaData())
            .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(configurations, followedLeaderIndices))
            .build());
        return newState.build();
    }

    @Override
    protected ClusterBlockException checkBlock(PutAutoFollowPatternAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
