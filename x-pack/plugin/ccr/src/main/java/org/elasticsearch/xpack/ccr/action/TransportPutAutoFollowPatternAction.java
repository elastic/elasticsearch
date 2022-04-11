/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
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
        final CcrLicenseChecker ccrLicenseChecker
    ) {
        super(
            PutAutoFollowPatternAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutAutoFollowPatternAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.client = client;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker, "ccrLicenseChecker");
    }

    @Override
    protected void masterOperation(
        Task task,
        PutAutoFollowPatternAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
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
        final Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(
            threadPool.getThreadContext(),
            clusterService.state()
        );

        Consumer<ClusterStateResponse> consumer = remoteClusterState -> {
            String[] indices = request.getLeaderIndexPatterns().toArray(new String[0]);
            ccrLicenseChecker.hasPrivilegesToFollowIndices(remoteClient, indices, e -> {
                if (e == null) {
                    clusterService.submitStateUpdateTask(
                        "put-auto-follow-pattern-" + request.getRemoteCluster(),
                        new AckedClusterStateUpdateTask(request, listener) {
                            @Override
                            public ClusterState execute(ClusterState currentState) {
                                return innerPut(request, filteredHeaders, currentState, remoteClusterState.getState());
                            }
                        },
                        newExecutor()
                    );
                } else {
                    listener.onFailure(e);
                }
            });
        };

        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metadata(true);

        CcrLicenseChecker.checkRemoteClusterLicenseAndFetchClusterState(
            client,
            request.getRemoteCluster(),
            clusterStateRequest,
            listener::onFailure,
            consumer
        );

    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }

    static ClusterState innerPut(
        PutAutoFollowPatternAction.Request request,
        Map<String, String> filteredHeaders,
        ClusterState localState,
        ClusterState remoteClusterState
    ) {
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
            markExistingIndicesAsAutoFollowedForNewPatterns(
                request.getLeaderIndexPatterns(),
                request.getLeaderIndexExclusionPatterns(),
                remoteClusterState.metadata(),
                previousPattern,
                followedIndexUUIDs
            );
        } else {
            markExistingIndicesAsAutoFollowed(
                request.getLeaderIndexPatterns(),
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
            request.getParameters().getReadPollTimeout()
        );
        patterns.put(request.getName(), autoFollowPattern);

        return localState.copyAndUpdateMetadata(
            metadata -> metadata.putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, followedLeaderIndices, headers))
        );
    }

    private static void markExistingIndicesAsAutoFollowedForNewPatterns(
        List<String> leaderIndexPatterns,
        List<String> leaderIndexExclusionPatterns,
        Metadata leaderMetadata,
        AutoFollowPattern previousPattern,
        List<String> followedIndexUUIDS
    ) {

        final List<String> newPatterns = leaderIndexPatterns.stream()
            .filter(p -> previousPattern.getLeaderIndexPatterns().contains(p) == false)
            .collect(Collectors.toList());
        markExistingIndicesAsAutoFollowed(newPatterns, leaderIndexExclusionPatterns, leaderMetadata, followedIndexUUIDS);
    }

    private static void markExistingIndicesAsAutoFollowed(
        List<String> patterns,
        List<String> exclusionPatterns,
        Metadata leaderMetadata,
        List<String> followedIndexUUIDS
    ) {

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
