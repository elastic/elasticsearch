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
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStream.BACKING_INDEX_PREFIX;

public final class TransportPutFollowAction extends TransportMasterNodeAction<PutFollowAction.Request, PutFollowAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutFollowAction.class);

    private final IndexScopedSettings indexScopedSettings;
    private final Client client;
    private final RestoreService restoreService;
    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportPutFollowAction(
        final ThreadPool threadPool,
        final TransportService transportService,
        final ClusterService clusterService,
        final IndexScopedSettings indexScopedSettings,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Client client,
        final RestoreService restoreService,
        final CcrLicenseChecker ccrLicenseChecker
    ) {
        super(
            PutFollowAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutFollowAction.Request::new,
            indexNameExpressionResolver,
            PutFollowAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.indexScopedSettings = indexScopedSettings;
        this.client = client;
        this.restoreService = restoreService;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    @Override
    protected void masterOperation(
        Task task,
        final PutFollowAction.Request request,
        final ClusterState state,
        final ActionListener<PutFollowAction.Response> listener
    ) {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        String remoteCluster = request.getRemoteCluster();
        // Validates whether the leader cluster has been configured properly:
        client.getRemoteClusterClient(remoteCluster);

        String leaderIndex = request.getLeaderIndex();
        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
            client,
            remoteCluster,
            leaderIndex,
            listener::onFailure,
            (historyUUID, tuple) -> createFollowerIndex(tuple.v1(), tuple.v2(), request, listener)
        );
    }

    private void createFollowerIndex(
        final IndexMetadata leaderIndexMetadata,
        final DataStream remoteDataStream,
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener
    ) {
        if (leaderIndexMetadata == null) {
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not exist"));
            return;
        }
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(leaderIndexMetadata.getSettings()) == false) {
            listener.onFailure(
                new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not have soft deletes enabled")
            );
            return;
        }
        if (leaderIndexMetadata.isSearchableSnapshot()) {
            listener.onFailure(
                new IllegalArgumentException(
                    "leader index ["
                        + request.getLeaderIndex()
                        + "] is a searchable snapshot index and cannot be used as a leader index for cross-cluster replication purpose"
                )
            );
            return;
        }

        final Settings replicatedRequestSettings = TransportResumeFollowAction.filter(request.getSettings());
        if (replicatedRequestSettings.isEmpty() == false) {
            final List<String> unknownKeys = replicatedRequestSettings.keySet()
                .stream()
                .filter(s -> indexScopedSettings.get(s) == null)
                .collect(Collectors.toList());
            final String message;
            if (unknownKeys.isEmpty()) {
                message = String.format(
                    Locale.ROOT,
                    "can not put follower index that could override leader settings %s",
                    replicatedRequestSettings
                );
            } else {
                message = String.format(
                    Locale.ROOT,
                    "unknown setting%s [%s]",
                    unknownKeys.size() == 1 ? "" : "s",
                    String.join(",", unknownKeys)
                );
            }
            listener.onFailure(new IllegalArgumentException(message));
            return;
        }

        final Settings overrideSettings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, request.getFollowerIndex())
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
            .put(request.getSettings())
            .build();

        final String leaderClusterRepoName = CcrRepository.NAME_PREFIX + request.getRemoteCluster();
        final RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(leaderClusterRepoName, CcrRepository.LATEST).indices(
            request.getLeaderIndex()
        )
            .indicesOptions(request.indicesOptions())
            .renamePattern("^(.*)$")
            .renameReplacement(request.getFollowerIndex())
            .masterNodeTimeout(request.masterNodeTimeout())
            .indexSettings(overrideSettings)
            .quiet(true);

        final Client clientWithHeaders = CcrLicenseChecker.wrapClient(
            this.client,
            threadPool.getThreadContext().getHeaders(),
            clusterService.state()
        );
        ActionListener<RestoreService.RestoreCompletionResponse> delegatelistener = listener.delegateFailure(
            (delegatedListener, response) -> afterRestoreStarted(clientWithHeaders, request, delegatedListener, response)
        );
        if (remoteDataStream == null) {
            // If the index we're following is not part of a data stream, start the
            // restoration of the index normally.
            restoreService.restoreSnapshot(restoreRequest, delegatelistener);
        } else {
            String followerIndexName = request.getFollowerIndex();
            // This method is used to update the metadata in the same cluster state
            // update as the snapshot is restored.
            BiConsumer<ClusterState, Metadata.Builder> updater = (currentState, mdBuilder) -> {
                final String localDataStreamName;

                // If we have been given a data stream name, use that name for the local
                // data stream. See the javadoc for AUTO_FOLLOW_PATTERN_REPLACEMENT
                // for more info.
                final String dsName = request.getDataStreamName();
                if (Strings.hasText(dsName)) {
                    localDataStreamName = dsName;
                } else {
                    // There was no specified name, use the original data stream name.
                    localDataStreamName = remoteDataStream.getName();
                }
                final DataStream localDataStream = mdBuilder.dataStreamMetadata().dataStreams().get(localDataStreamName);
                final Index followerIndex = mdBuilder.get(followerIndexName).getIndex();
                assert followerIndex != null : "expected followerIndex " + followerIndexName + " to exist in the state, but it did not";

                final DataStream updatedDataStream = updateLocalDataStream(
                    followerIndex,
                    localDataStream,
                    localDataStreamName,
                    remoteDataStream
                );
                mdBuilder.put(updatedDataStream);
            };
            restoreService.restoreSnapshot(restoreRequest, delegatelistener, updater);
        }
    }

    private void afterRestoreStarted(
        Client clientWithHeaders,
        PutFollowAction.Request request,
        ActionListener<PutFollowAction.Response> originalListener,
        RestoreService.RestoreCompletionResponse response
    ) {
        final ActionListener<PutFollowAction.Response> listener;
        if (ActiveShardCount.NONE.equals(request.waitForActiveShards())) {
            originalListener.onResponse(new PutFollowAction.Response(true, false, false));
            listener = new ActionListener<>() {

                @Override
                public void onResponse(PutFollowAction.Response response) {
                    logger.debug("put follow {} completed with {}", request, response);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> "put follow " + request + " failed during the restore process", e);
                }
            };
        } else {
            listener = originalListener;
        }

        RestoreClusterStateListener.createAndRegisterListener(
            clusterService,
            response,
            listener.delegateFailure((delegatedListener, restoreSnapshotResponse) -> {
                RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
                if (restoreInfo == null) {
                    // If restoreInfo is null then it is possible there was a master failure during the
                    // restore.
                    delegatedListener.onResponse(new PutFollowAction.Response(true, false, false));
                } else if (restoreInfo.failedShards() == 0) {
                    initiateFollowing(clientWithHeaders, request, delegatedListener);
                } else {
                    assert restoreInfo.failedShards() > 0 : "Should have failed shards";
                    delegatedListener.onResponse(new PutFollowAction.Response(true, false, false));
                }
            }),
            threadPool.getThreadContext()
        );
    }

    private void initiateFollowing(
        final Client clientWithHeaders,
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener
    ) {
        assert request.waitForActiveShards() != ActiveShardCount.DEFAULT : "PutFollowAction does not support DEFAULT.";
        FollowParameters parameters = request.getParameters();
        ResumeFollowAction.Request resumeFollowRequest = new ResumeFollowAction.Request();
        resumeFollowRequest.setFollowerIndex(request.getFollowerIndex());
        resumeFollowRequest.setParameters(new FollowParameters(parameters));
        clientWithHeaders.execute(
            ResumeFollowAction.INSTANCE,
            resumeFollowRequest,
            listener.wrapFailure(
                (l, r) -> ActiveShardsObserver.waitForActiveShards(
                    clusterService,
                    new String[] { request.getFollowerIndex() },
                    request.waitForActiveShards(),
                    request.timeout(),
                    l.map(result -> new PutFollowAction.Response(true, result, r.isAcknowledged()))
                )
            )
        );
    }

    /**
     * Given the backing index that the follower is going to follow, the local data stream (if it
     * exists) and the remote data stream, return the new local data stream for the local cluster
     * (the follower) updated with whichever information is necessary to restore the new
     * soon-to-be-followed index.
     */
    static DataStream updateLocalDataStream(
        Index backingIndexToFollow,
        DataStream localDataStream,
        String localDataStreamName,
        DataStream remoteDataStream
    ) {
        if (localDataStream == null) {
            // The data stream and the backing indices have been created and validated in the remote cluster,
            // just copying the data stream is in this case safe.
            return new DataStream(
                localDataStreamName,
                List.of(backingIndexToFollow),
                remoteDataStream.getGeneration(),
                remoteDataStream.getMetadata(),
                remoteDataStream.isHidden(),
                true,
                remoteDataStream.isSystem(),
                remoteDataStream.isAllowCustomRouting(),
                remoteDataStream.getIndexMode(),
                remoteDataStream.getLifecycle()
            );
        } else {
            if (localDataStream.isReplicated() == false) {
                throw new IllegalArgumentException(
                    "cannot follow backing index ["
                        + backingIndexToFollow.getName()
                        + "], because local data stream ["
                        + localDataStream.getName()
                        + "] is no longer marked as replicated"
                );
            }

            final List<Index> backingIndices;
            if (localDataStream.getIndices().contains(backingIndexToFollow) == false) {
                backingIndices = new ArrayList<>(localDataStream.getIndices());
                backingIndices.add(backingIndexToFollow);
                // When following an older backing index it should be positioned before the newer backing indices.
                // Currently the assumption is that the newest index (highest generation) is the write index.
                // (just appending an older backing index to the list of backing indices would break that assumption)
                // Not all backing indices follow the data stream backing indices naming convention (e.g. some might start with
                // "restored-" if they're mounted indices, "shrink-" if they were shrunk, or miscellaneously named indices could be added
                // to the data stream using the modify data stream API) so we use a comparator that partitions the non-standard backing
                // indices at the beginning of the data stream (lower generations) and sorts them amongst themselves, and the rest of the
                // indices (that contain `.ds-`) are sorted based on the original backing index name (ie. ignoring everything up to `.ds-`)
                // The goal is to make sure the prefixed (usually read-only - shrink-, restored-, partial-) backing indices do not end
                // up being the write index of the local data stream.

                String partitionByBackingIndexBaseName = BACKING_INDEX_PREFIX + localDataStream.getName();
                backingIndices.sort(
                    Comparator.comparing((Index o) -> o.getName().contains(partitionByBackingIndexBaseName) ? 1 : -1)
                        .thenComparing((Index o) -> {
                            int backingPrefixPosition = o.getName().indexOf(BACKING_INDEX_PREFIX);
                            return backingPrefixPosition > -1 ? o.getName().substring(backingPrefixPosition) : o.getName();
                        })
                );
            } else {
                // edge case where the index was closed on the follower and was already in the datastream's index list
                backingIndices = localDataStream.getIndices();
            }

            return new DataStream(
                localDataStream.getName(),
                backingIndices,
                remoteDataStream.getGeneration(),
                remoteDataStream.getMetadata(),
                localDataStream.isHidden(),
                localDataStream.isReplicated(),
                localDataStream.isSystem(),
                localDataStream.isAllowCustomRouting(),
                localDataStream.getIndexMode(),
                localDataStream.getLifecycle()
            );
        }
    }

    @Override
    protected ClusterBlockException checkBlock(final PutFollowAction.Request request, final ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowerIndex());
    }
}
