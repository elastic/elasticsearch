/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TransportPutAutoFollowPatternAction extends
    TransportMasterNodeAction<PutAutoFollowPatternAction.Request, AcknowledgedResponse> {

    private final Client client;
    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportPutAutoFollowPatternAction(
            final TransportService transportService,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ActionFilters actionFilters,
            final Client client,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final CcrLicenseChecker ccrLicenseChecker) {
        super(PutAutoFollowPatternAction.NAME, transportService, clusterService, threadPool, actionFilters,
            PutAutoFollowPatternAction.Request::new, indexNameExpressionResolver);
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
        final Client remoteClient = client.getRemoteClusterClient(request.getBody().getRemoteCluster());
        final Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
            .filter(e -> ShardFollowTask.HEADER_FILTERS.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Consumer<ClusterStateResponse> consumer = remoteClusterState -> {
            String[] indices = request.getBody().getLeaderIndexPatterns().toArray(new String[0]);
            ccrLicenseChecker.hasPrivilegesToFollowIndices(remoteClient, indices, e -> {
                if (e == null) {
                    clusterService.submitStateUpdateTask("put-auto-follow-pattern-" + request.getBody().getRemoteCluster(),
                        new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

                            @Override
                            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                                return new AcknowledgedResponse(acknowledged);
                            }

                            @Override
                            public ClusterState execute(ClusterState currentState) throws Exception {
                                return innerPut(request, filteredHeaders, currentState, remoteClusterState.getState());
                            }
                        });
                } else {
                    listener.onFailure(e);
                }
            });
        };

        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metaData(true);

        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchClusterState(client, request.getBody().getRemoteCluster(),
            clusterStateRequest, listener::onFailure, consumer);

    }

    static ClusterState innerPut(PutAutoFollowPatternAction.Request request,
                                 Map<String, String> filteredHeaders,
                                 ClusterState localState,
                                 ClusterState remoteClusterState) {
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

        AutoFollowPattern previousPattern = patterns.get(request.getBody().getName());
        final List<String> followedIndexUUIDs;
        if (followedLeaderIndices.containsKey(request.getBody().getName())) {
            followedIndexUUIDs = new ArrayList<>(followedLeaderIndices.get(request.getBody().getName()));
        } else {
            followedIndexUUIDs = new ArrayList<>();
        }
        followedLeaderIndices.put(request.getBody().getName(), followedIndexUUIDs);
        // Mark existing leader indices as already auto followed:
        if (previousPattern != null) {
            markExistingIndicesAsAutoFollowedForNewPatterns(request.getBody().getLeaderIndexPatterns(), remoteClusterState.metaData(),
                previousPattern, followedIndexUUIDs);
        } else {
            markExistingIndicesAsAutoFollowed(request.getBody().getLeaderIndexPatterns(), remoteClusterState.metaData(),
                followedIndexUUIDs);
        }

        if (filteredHeaders != null) {
            headers.put(request.getBody().getName(), filteredHeaders);
        }

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern(
            request.getBody().getRemoteCluster(),
            request.getBody().getLeaderIndexPatterns(),
            request.getBody().getFollowIndexNamePattern(),
            request.getBody().getMaxReadRequestOperationCount(),
            request.getBody().getMaxReadRequestSize(),
            request.getBody().getMaxOutstandingReadRequests(),
            request.getBody().getMaxWriteRequestOperationCount(),
            request.getBody().getMaxWriteRequestSize(),
            request.getBody().getMaxOutstandingWriteRequests(),
            request.getBody().getMaxWriteBufferCount(),
            request.getBody().getMaxWriteBufferSize(),
            request.getBody().getMaxRetryDelay(),
            request.getBody().getReadPollTimeout());
        patterns.put(request.getBody().getName(), autoFollowPattern);
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
