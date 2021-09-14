/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
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

public final class TransportPutFollowAction
    extends TransportMasterNodeAction<PutFollowAction.Request, PutFollowAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutFollowAction.class);

    private final IndexScopedSettings indexScopedSettings;
    private final Client client;
    private final RestoreService restoreService;
    private final CcrLicenseChecker ccrLicenseChecker;
    private final ActiveShardsObserver activeShardsObserver;

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
            final CcrLicenseChecker ccrLicenseChecker) {
        super(
                PutFollowAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                PutFollowAction.Request::new,
                indexNameExpressionResolver,
                PutFollowAction.Response::new,
                ThreadPool.Names.SAME);
        this.indexScopedSettings = indexScopedSettings;
        this.client = client;
        this.restoreService = restoreService;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
    }

    @Override
    protected void masterOperation(
        Task task, final PutFollowAction.Request request,
        final ClusterState state,
        final ActionListener<PutFollowAction.Response> listener) {
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
            (historyUUID, tuple) -> createFollowerIndex(tuple.v1(), tuple.v2(), request, listener));
    }

    private void createFollowerIndex(
            final IndexMetadata leaderIndexMetadata,
            final DataStream remoteDataStream,
            final PutFollowAction.Request request,
            final ActionListener<PutFollowAction.Response> listener) {
        if (leaderIndexMetadata == null) {
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not exist"));
            return;
        }
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(leaderIndexMetadata.getSettings()) == false) {
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getLeaderIndex() +
                "] does not have soft deletes enabled"));
            return;
        }
        if (SearchableSnapshotsSettings.isSearchableSnapshotStore(leaderIndexMetadata.getSettings())) {
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getLeaderIndex() +
                "] is a searchable snapshot index and cannot be used as a leader index for cross-cluster replication purpose"));
            return;
        }

        final Settings replicatedRequestSettings = TransportResumeFollowAction.filter(request.getSettings());
        if (replicatedRequestSettings.isEmpty() == false) {
            final List<String> unknownKeys =
                replicatedRequestSettings.keySet().stream().filter(s -> indexScopedSettings.get(s) == null).collect(Collectors.toList());
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

        if (remoteDataStream != null) {
            // when following a backing index then the names of the backing index must be remain the same in the local
            // and remote cluster.
            if (request.getLeaderIndex().equals(request.getFollowerIndex()) == false) {
                listener.onFailure(
                    new IllegalArgumentException("a backing index name in the local and remote cluster must remain the same"));
                return;
            }
        }

        final Settings overrideSettings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, request.getFollowerIndex())
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
            .put(request.getSettings())
            .build();

        final String leaderClusterRepoName = CcrRepository.NAME_PREFIX + request.getRemoteCluster();
        final RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(leaderClusterRepoName, CcrRepository.LATEST)
            .indices(request.getLeaderIndex()).indicesOptions(request.indicesOptions()).renamePattern("^(.*)$")
            .renameReplacement(request.getFollowerIndex()).masterNodeTimeout(request.masterNodeTimeout())
            .indexSettings(overrideSettings);

        final Client clientWithHeaders = CcrLicenseChecker.wrapClient(this.client, threadPool.getThreadContext().getHeaders());
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            protected void doRun() {
                ActionListener<RestoreService.RestoreCompletionResponse> delegatelistener = listener.delegateFailure(
                        (delegatedListener, response) -> afterRestoreStarted(clientWithHeaders, request, delegatedListener, response));
                if (remoteDataStream == null) {
                    restoreService.restoreSnapshot(restoreRequest, delegatelistener);
                } else {
                    String followerIndexName = request.getFollowerIndex();
                    BiConsumer<ClusterState, Metadata.Builder> updater = (currentState, mdBuilder) -> {
                        DataStream localDataStream = currentState.getMetadata().dataStreams().get(remoteDataStream.getName());
                        Index followerIndex = mdBuilder.get(followerIndexName).getIndex();
                        assert followerIndex != null;

                        DataStream updatedDataStream = updateLocalDataStream(followerIndex, localDataStream, remoteDataStream);
                        mdBuilder.put(updatedDataStream);
                    };
                    restoreService.restoreSnapshot(restoreRequest, delegatelistener, updater);
                }
            }
        });
    }

    private void afterRestoreStarted(Client clientWithHeaders, PutFollowAction.Request request,
                                     ActionListener<PutFollowAction.Response> originalListener,
                                     RestoreService.RestoreCompletionResponse response) {
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
                    logger.debug(() -> new ParameterizedMessage("put follow {} failed during the restore process", request), e);
                }
            };
        } else {
            listener = originalListener;
        }

        RestoreClusterStateListener.createAndRegisterListener(clusterService, response, listener.delegateFailure(
            (delegatedListener, restoreSnapshotResponse) -> {
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
            }));
    }

    private void initiateFollowing(
        final Client client,
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener) {
        assert request.waitForActiveShards() != ActiveShardCount.DEFAULT : "PutFollowAction does not support DEFAULT.";
        FollowParameters parameters = request.getParameters();
        ResumeFollowAction.Request resumeFollowRequest = new ResumeFollowAction.Request();
        resumeFollowRequest.setFollowerIndex(request.getFollowerIndex());
        resumeFollowRequest.setParameters(new FollowParameters(parameters));
        client.execute(ResumeFollowAction.INSTANCE, resumeFollowRequest, ActionListener.wrap(
            r -> activeShardsObserver.waitForActiveShards(new String[]{request.getFollowerIndex()},
                request.waitForActiveShards(), request.timeout(), result ->
                    listener.onResponse(new PutFollowAction.Response(true, result, r.isAcknowledged())),
                listener::onFailure),
            listener::onFailure
        ));
    }

    static DataStream updateLocalDataStream(Index backingIndexToFollow,
                                            DataStream localDataStream,
                                            DataStream remoteDataStream) {
        if (localDataStream == null) {
            // The data stream and the backing indices have been created and validated in the remote cluster,
            // just copying the data stream is in this case safe.
            return new DataStream(remoteDataStream.getName(), remoteDataStream.getTimeStampField(),
                List.of(backingIndexToFollow), remoteDataStream.getGeneration(), remoteDataStream.getMetadata(),
                remoteDataStream.isHidden(), true);
        } else {
            if (localDataStream.isReplicated() == false) {
                throw new IllegalArgumentException("cannot follow backing index [" + backingIndexToFollow.getName() +
                    "], because local data stream [" + localDataStream.getName() + "] is no longer marked as replicated");
            }

            List<Index> backingIndices = new ArrayList<>(localDataStream.getIndices());
            backingIndices.add(backingIndexToFollow);

            // When following an older backing index it should be positioned before the newer backing indices.
            // Currently the assumption is that the newest index (highest generation) is the write index.
            // (just appending an older backing index to the list of backing indices would break that assumption)
            // (string sorting works because of the naming backing index naming scheme)
            backingIndices.sort(Comparator.comparing(Index::getName));

            return new DataStream(localDataStream.getName(), localDataStream.getTimeStampField(), backingIndices,
                remoteDataStream.getGeneration(), remoteDataStream.getMetadata(), localDataStream.isHidden(),
                localDataStream.isReplicated());
        }
    }

    @Override
    protected ClusterBlockException checkBlock(final PutFollowAction.Request request, final ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowerIndex());
    }
}
