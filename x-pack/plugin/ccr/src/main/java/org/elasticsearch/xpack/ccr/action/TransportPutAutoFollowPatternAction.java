/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TransportPutAutoFollowPatternAction extends AcknowledgedTransportMasterNodeAction<PutAutoFollowPatternAction.Request> {
    private static final Logger logger = LogManager.getLogger(TransportPutAutoFollowPatternAction.class);

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
            PutAutoFollowPatternAction.Request::new, indexNameExpressionResolver, ThreadPool.Names.SAME);
        this.client = client;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker, "ccrLicenseChecker");
    }

    @Override
    protected void masterOperation(Task task, PutAutoFollowPatternAction.Request request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }

        final Settings replicatedRequestSettings = TransportResumeFollowAction.filter(request.getSettings());
        if (replicatedRequestSettings.isEmpty() == false) {
            final String message = String.format(
                Locale.ROOT,
                "can not put auto-follow pattern that could override leader settings %s",
                replicatedRequestSettings
            );
            listener.onFailure(new IllegalArgumentException(message));
            return;
        }
        final Client remoteClient = client.getRemoteClusterClient(request.getRemoteCluster());
        final Map<String, String> filteredHeaders = ClientHelper.filterSecurityHeaders(threadPool.getThreadContext().getHeaders());

        Consumer<ClusterStateResponse> consumer = remoteClusterState -> {
            String[] indices = request.getLeaderIndexPatterns().toArray(new String[0]);
            ccrLicenseChecker.hasPrivilegesToFollowIndices(remoteClient, indices, e -> {
                if (e == null) {
                    clusterService.submitStateUpdateTask("put-auto-follow-pattern-" + request.getRemoteCluster(),
                        new AckedClusterStateUpdateTask(request, listener) {
                            @Override
                            public ClusterState execute(ClusterState currentState) {
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
        clusterStateRequest.metadata(true);

        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchClusterState(client, request.getRemoteCluster(),
            clusterStateRequest, listener::onFailure, consumer);

    }

    static ClusterState innerPut(PutAutoFollowPatternAction.Request request,
                                 Map<String, String> filteredHeaders,
                                 ClusterState localState,
                                 ClusterState remoteClusterState) {
        // auto patterns are always overwritten
        // only already followed index uuids are updated

        AutoFollowMetadata currentAutoFollowMetadata = localState.metadata().custom(AutoFollowMetadata.TYPE);
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

        AutoFollowPattern previousPattern = patterns.get(request.getName());
        final List<String> followedIndexUUIDs;
        if (followedLeaderIndices.containsKey(request.getName())) {
            followedIndexUUIDs = new ArrayList<>(followedLeaderIndices.get(request.getName()));
        } else {
            followedIndexUUIDs = new ArrayList<>();
        }
        followedLeaderIndices.put(request.getName(), followedIndexUUIDs);
        // Mark existing leader indices as already auto followed:
        if (previousPattern != null) {
            markExistingIndicesAsAutoFollowedForNewPatterns(request.getLeaderIndexPatterns(),
                request.getLeaderIndexExclusionPatterns(),
                remoteClusterState.metadata(),
                previousPattern,
                followedIndexUUIDs
            );

            warnIfFollowedIndicesExcludedWithNewPatterns(request.getName(),
                request.getLeaderIndexPatterns(),
                request.getLeaderIndexExclusionPatterns(),
                remoteClusterState.metadata(),
                localState.metadata(),
                previousPattern,
                followedIndexUUIDs);
        } else {
            markExistingIndicesAsAutoFollowed(request.getLeaderIndexPatterns(),
                request.getLeaderIndexExclusionPatterns(),
                remoteClusterState.metadata(),
                followedIndexUUIDs
            );
        }

        if (filteredHeaders != null) {
            headers.put(request.getName(), filteredHeaders);
        }

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern(
            request.getRemoteCluster(),
            request.getLeaderIndexPatterns(),
            request.getLeaderIndexExclusionPatterns(),
            request.getFollowIndexNamePattern(),
            request.getSettings(),
            true,
            request.getParameters().getMaxReadRequestOperationCount(),
            request.getParameters().getMaxWriteRequestOperationCount(),
            request.getParameters().getMaxOutstandingReadRequests(),
            request.getParameters().getMaxOutstandingWriteRequests(),
            request.getParameters().getMaxReadRequestSize(),
            request.getParameters().getMaxWriteRequestSize(),
            request.getParameters().getMaxWriteBufferCount(),
            request.getParameters().getMaxWriteBufferSize(),
            request.getParameters().getMaxRetryDelay(),
            request.getParameters().getReadPollTimeout());
        patterns.put(request.getName(), autoFollowPattern);
        ClusterState.Builder newState = ClusterState.builder(localState);
        newState.metadata(Metadata.builder(localState.getMetadata())
            .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, followedLeaderIndices, headers))
            .build());
        return newState.build();
    }

    private static void markExistingIndicesAsAutoFollowedForNewPatterns(
        List<String> leaderIndexPatterns,
        List<String> leaderIndexExclusionPatterns,
        Metadata leaderMetadata,
        AutoFollowPattern previousPattern,
        List<String> followedIndexUUIDS) {

        final List<String> newPatterns = leaderIndexPatterns
            .stream()
            .filter(p -> previousPattern.getLeaderIndexPatterns().contains(p) == false)
            .collect(Collectors.toList());
        markExistingIndicesAsAutoFollowed(newPatterns, leaderIndexExclusionPatterns, leaderMetadata, followedIndexUUIDS);
    }

    private static void warnIfFollowedIndicesExcludedWithNewPatterns(String autoFollowName,
                                                                     List<String> newLeaderPatterns,
                                                                     List<String> newLeaderIndexExclusionPatterns,
                                                                     Metadata remoteMetadata,
                                                                     Metadata localMetadata,
                                                                     AutoFollowPattern previousPattern,
                                                                     List<String> followedIndexUUIDS) {
        final boolean hasNewExclusionPatterns = newLeaderIndexExclusionPatterns
            .stream()
            .anyMatch(p -> previousPattern.getLeaderIndexExclusionPatterns().contains(p) == false);

        if (hasNewExclusionPatterns == false) {
            return;
        }

        for (IndexMetadata localIndexMetadata : localMetadata) {
            final Map<String, String> ccrMetadata = localIndexMetadata.getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
            if (ccrMetadata != null && followedIndexUUIDS.contains(ccrMetadata.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY))) {
                final String leaderIndexName = ccrMetadata.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY);
                IndexAbstraction indexAbstraction = remoteMetadata.getIndicesLookup().get(leaderIndexName);
                final IndexMetadata leaderIndexMetadata = remoteMetadata.index(leaderIndexName);

                if (AutoFollowPattern.match(newLeaderPatterns, newLeaderIndexExclusionPatterns, indexAbstraction) == false) {
                    logger.warn("The follower index {} for leader index {} does not match against the updated auto follow " +
                            "pattern with name {}, follow patterns {} and exclusion patterns {}",
                        localIndexMetadata.getIndex(),
                        leaderIndexMetadata.getIndex(),
                        autoFollowName,
                        newLeaderPatterns,
                        newLeaderIndexExclusionPatterns);
                }
            }
        }
    }

    private static void markExistingIndicesAsAutoFollowed(
        List<String> patterns,
        List<String> exclusionPatterns,
        Metadata leaderMetadata,
        List<String> followedIndexUUIDS) {

        for (final IndexMetadata indexMetadata : leaderMetadata) {
            IndexAbstraction indexAbstraction = leaderMetadata.getIndicesLookup().get(indexMetadata.getIndex().getName());
            if (AutoFollowPattern.match(patterns, exclusionPatterns, indexAbstraction)) {
                followedIndexUUIDS.add(indexMetadata.getIndexUUID());
            }
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutAutoFollowPatternAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
