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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TransportPutAutoFollowPatternAction extends
    TransportMasterNodeAction<PutAutoFollowPatternAction.Request, AcknowledgedResponse> {

    private final Client client;
    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportPutAutoFollowPatternAction(
            final Settings settings,
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ActionFilters actionFilters,
            final Client client,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final CcrLicenseChecker ccrLicenseChecker) {
        super(settings, PutAutoFollowPatternAction.NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, PutAutoFollowPatternAction.Request::new);
        this.client = client;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker, "ccrLicenseChecker");
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
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        final Client leaderClient;
        if (request.getLeaderClusterAlias().equals("_local_")) {
            leaderClient = client;
        } else {
            leaderClient = client.getRemoteClusterClient(request.getLeaderClusterAlias());
        }

        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metaData(true);

        Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
            .filter(e -> ShardFollowTask.HEADER_FILTERS.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        String[] indices = request.getLeaderIndexPatterns().toArray(new String[0]);
        ccrLicenseChecker.hasPrivilegesToFollowIndices(leaderClient, indices, e -> {
            if (e == null) {
                leaderClient.admin().cluster().state(
                    clusterStateRequest,
                    ActionListener.wrap(
                        clusterStateResponse -> {
                            final ClusterState leaderClusterState = clusterStateResponse.getState();
                            clusterService.submitStateUpdateTask("put-auto-follow-pattern-" + request.getLeaderClusterAlias(),
                                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

                                    @Override
                                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                                        return new AcknowledgedResponse(acknowledged);
                                    }

                                    @Override
                                    public ClusterState execute(ClusterState currentState) throws Exception {
                                        return innerPut(request, filteredHeaders, currentState, leaderClusterState);
                                    }
                                });
                        },
                        listener::onFailure));
            } else {
                listener.onFailure(e);
            }
        });
    }

    static ClusterState innerPut(PutAutoFollowPatternAction.Request request,
                                 Map<String, String> filteredHeaders,
                                 ClusterState localState,
                                 ClusterState leaderClusterState) {
        // auto patterns are always overwritten
        // only already followed index uuids are updated

        AutoFollowMetadata currentAutoFollowMetadata = localState.metaData().custom(AutoFollowMetadata.TYPE);
        Map<String, List<String>> followedLeaderIndices;
        Map<String, AutoFollowPattern> patterns;
        Map<String, Map<String, String>> headers;
        if (currentAutoFollowMetadata != null) {
            patterns = new HashMap<>(currentAutoFollowMetadata.getPatterns());
            followedLeaderIndices = new HashMap<>(currentAutoFollowMetadata.getFollowedLeaderIndexUUIDs());
            headers = new HashMap<>(currentAutoFollowMetadata.getHeaders());
        } else {
            patterns = new HashMap<>();
            followedLeaderIndices = new HashMap<>();
            headers = new HashMap<>();
        }

        AutoFollowPattern previousPattern = patterns.get(request.getLeaderClusterAlias());
        final List<String> followedIndexUUIDs;
        if (followedLeaderIndices.containsKey(request.getLeaderClusterAlias())) {
            followedIndexUUIDs = new ArrayList<>(followedLeaderIndices.get(request.getLeaderClusterAlias()));
        } else {
            followedIndexUUIDs = new ArrayList<>();
        }
        followedLeaderIndices.put(request.getLeaderClusterAlias(), followedIndexUUIDs);
        // Mark existing leader indices as already auto followed:
        if (previousPattern != null) {
            markExistingIndicesAsAutoFollowedForNewPatterns(request.getLeaderIndexPatterns(), leaderClusterState.metaData(),
                previousPattern, followedIndexUUIDs);
        } else {
            markExistingIndicesAsAutoFollowed(request.getLeaderIndexPatterns(), leaderClusterState.metaData(),
                followedIndexUUIDs);
        }

        if (filteredHeaders != null) {
            headers.put(request.getLeaderClusterAlias(), filteredHeaders);
        }

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern(
            request.getLeaderIndexPatterns(),
            request.getFollowIndexNamePattern(),
            request.getMaxBatchOperationCount(),
            request.getMaxConcurrentReadBatches(),
            request.getMaxOperationSizeInBytes(),
            request.getMaxConcurrentWriteBatches(),
            request.getMaxWriteBufferSize(),
            request.getMaxRetryDelay(),
            request.getPollTimeout());
        patterns.put(request.getLeaderClusterAlias(), autoFollowPattern);
        ClusterState.Builder newState = ClusterState.builder(localState);
        newState.metaData(MetaData.builder(localState.getMetaData())
            .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, followedLeaderIndices, headers))
            .build());
        return newState.build();
    }

    private static void markExistingIndicesAsAutoFollowedForNewPatterns(
        List<String> leaderIndexPatterns,
        MetaData leaderMetaData,
        AutoFollowPattern previousPattern,
        List<String> followedIndexUUIDS) {

        final List<String> newPatterns = leaderIndexPatterns
            .stream()
            .filter(p -> previousPattern.getLeaderIndexPatterns().contains(p) == false)
            .collect(Collectors.toList());
        markExistingIndicesAsAutoFollowed(newPatterns, leaderMetaData, followedIndexUUIDS);
    }

    private static void markExistingIndicesAsAutoFollowed(
        List<String> patterns,
        MetaData leaderMetaData,
        List<String> followedIndexUUIDS) {

        for (final IndexMetaData indexMetaData : leaderMetaData) {
            if (AutoFollowPattern.match(patterns, indexMetaData.getIndex().getName())) {
                followedIndexUUIDS.add(indexMetaData.getIndexUUID());
            }
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutAutoFollowPatternAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
