/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.SingleResultDeduplicator;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.UnsafePlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.elasticsearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.shard.IndexShardRecoveryException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.MultiChunkTransfer;
import org.elasticsearch.indices.recovery.MultiFileWriter;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.blobstore.FileRestoreContext;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrRetentionLeases;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.action.CcrRequests;
import org.elasticsearch.xpack.ccr.action.repositories.ClearCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.action.repositories.ClearCcrRestoreSessionRequest;
import org.elasticsearch.xpack.ccr.action.repositories.GetCcrRestoreFileChunkAction;
import org.elasticsearch.xpack.ccr.action.repositories.GetCcrRestoreFileChunkRequest;
import org.elasticsearch.xpack.ccr.action.repositories.PutCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.action.repositories.PutCcrRestoreSessionRequest;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;
import static org.elasticsearch.repositories.RepositoryData.MISSING_UUID;
import static org.elasticsearch.xpack.ccr.CcrRetentionLeases.retentionLeaseId;
import static org.elasticsearch.xpack.ccr.CcrRetentionLeases.syncAddRetentionLease;
import static org.elasticsearch.xpack.ccr.CcrRetentionLeases.syncRenewRetentionLease;

/**
 * This repository relies on a remote cluster for Ccr restores. It is read-only so it can only be used to
 * restore shards/indexes that exist on the remote cluster.
 */
public class CcrRepository extends AbstractLifecycleComponent implements Repository {

    private static final Logger logger = LogManager.getLogger(CcrRepository.class);

    public static final String LATEST = "_latest_";
    public static final String TYPE = "_ccr_";
    public static final String NAME_PREFIX = "_ccr_";
    private static final SnapshotId SNAPSHOT_ID = new SnapshotId(LATEST, LATEST);
    private static final String IN_SYNC_ALLOCATION_ID = "ccr_restore";

    private final ProjectId projectId;
    private final RepositoryMetadata metadata;
    private final CcrSettings ccrSettings;
    private final String localClusterName;
    private final String remoteClusterAlias;
    private final Client client;
    private final ThreadPool threadPool;
    private final Executor remoteClientResponseExecutor;
    private final Executor chunkResponseExecutor;

    private final CounterMetric throttledTime = new CounterMetric();

    private final SingleResultDeduplicator<ClusterState> csDeduplicator;

    public CcrRepository(
        ProjectId projectId,
        RepositoryMetadata metadata,
        Client client,
        Settings settings,
        CcrSettings ccrSettings,
        ThreadPool threadPool
    ) {
        this.projectId = projectId;
        this.metadata = metadata;
        this.ccrSettings = ccrSettings;
        this.localClusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
        assert metadata.name().startsWith(NAME_PREFIX) : "CcrRepository metadata.name() must start with: " + NAME_PREFIX;
        this.remoteClusterAlias = Strings.split(metadata.name(), NAME_PREFIX)[1];
        this.client = client;
        this.threadPool = threadPool;
        this.remoteClientResponseExecutor = threadPool.executor(Ccr.CCR_THREAD_POOL_NAME);
        this.chunkResponseExecutor = threadPool.generic();
        csDeduplicator = new SingleResultDeduplicator<>(
            threadPool.getThreadContext(),
            l -> getRemoteClusterClient().execute(
                ClusterStateAction.REMOTE_TYPE,
                new ClusterStateRequest(TimeValue.MAX_VALUE).clear().metadata(true).nodes(true),
                l.map(ClusterStateResponse::getState)
            )
        );
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

    @Override
    public ProjectId getProjectId() {
        return projectId;
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return metadata;
    }

    private RemoteClusterClient getRemoteClusterClient() {
        return client.getRemoteClusterClient(
            remoteClusterAlias,
            remoteClientResponseExecutor,
            RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
        );
    }

    @Override
    public void getSnapshotInfo(
        Collection<SnapshotId> snapshotIds,
        boolean abortOnFailure,
        BooleanSupplier isCancelled,
        CheckedConsumer<SnapshotInfo, Exception> consumer,
        ActionListener<Void> listener
    ) {
        assert snapshotIds.size() == 1 && SNAPSHOT_ID.equals(snapshotIds.iterator().next())
            : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId but saw " + snapshotIds;
        try {
            csDeduplicator.execute(
                new ThreadedActionListener<>(threadPool.executor(ThreadPool.Names.SNAPSHOT_META), listener.map(response -> {
                    Snapshot snapshot = new Snapshot(this.metadata.name(), SNAPSHOT_ID);

                    // RestoreService will validate the snapshot is restorable based on the max index version.
                    // However, we do not increment index version on every single release.
                    // To prevent attempting to restore an index of a future version, we reject the restore
                    // already when building the snapshot info from newer nodes matching the current index version.
                    IndexVersion maxIndexVersion = response.getNodes().getMaxDataNodeCompatibleIndexVersion();
                    if (IndexVersion.current().equals(maxIndexVersion)) {
                        for (var node : response.nodes()) {
                            if (node.canContainData() && node.getMaxIndexVersion().equals(maxIndexVersion)) {
                                BuildVersion remoteVersion = node.getBuildVersion();
                                if (remoteVersion.isFutureVersion()) {
                                    throw new SnapshotException(
                                        snapshot,
                                        "the snapshot was created with version ["
                                            + remoteVersion
                                            + "] which is higher than the version of this node ["
                                            + Build.current().version()
                                            + "]"
                                    );
                                }
                            }
                        }
                    }

                    Metadata responseMetadata = response.metadata();
                    Map<String, IndexMetadata> indicesMap = responseMetadata.getProject().indices();
                    consumer.accept(
                        new SnapshotInfo(
                            snapshot,
                            List.copyOf(indicesMap.keySet()),
                            List.copyOf(responseMetadata.getProject().dataStreams().keySet()),
                            List.of(),
                            maxIndexVersion,
                            SnapshotState.SUCCESS
                        )
                    );
                    return null;
                }))
            );
        } catch (Exception e) {
            assert false : e;
            listener.onFailure(e);
        }
    }

    private <Request extends ActionRequest, Response extends ActionResponse> Response executeRecoveryAction(
        RemoteClusterClient client,
        RemoteClusterActionType<Response> action,
        Request request
    ) {
        final var future = new PlainActionFuture<Response>();
        client.execute(action, request, future);
        // TODO stop doing this as a blocking activity
        // TODO on timeout, cancel the remote request, don't just carry on
        // TODO handle exceptions better, don't just unwrap/rewrap them with actionGet
        return future.actionGet(ccrSettings.getRecoveryActionTimeout().millis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        var remoteClient = getRemoteClusterClient();
        ClusterStateResponse clusterState = executeRecoveryAction(
            remoteClient,
            ClusterStateAction.REMOTE_TYPE,
            CcrRequests.metadataRequest(
                // We set a single dummy index name to avoid fetching all the index data
                "dummy_index_name"
            )
        );
        return clusterState.getState().metadata();
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        String leaderIndex = index.getName();
        var remoteClient = getRemoteClusterClient();

        ClusterStateResponse clusterState = executeRecoveryAction(
            remoteClient,
            ClusterStateAction.REMOTE_TYPE,
            CcrRequests.metadataRequest(leaderIndex)
        );

        // Validates whether the leader cluster has been configured properly:
        PlainActionFuture<String[]> future = new PlainActionFuture<>();
        IndexMetadata leaderIndexMetadata = clusterState.getState().metadata().getProject().index(leaderIndex);
        CcrLicenseChecker.fetchLeaderHistoryUUIDs(remoteClient, leaderIndexMetadata, future::onFailure, future::onResponse);
        String[] leaderHistoryUUIDs = future.actionGet(ccrSettings.getRecoveryActionTimeout());

        IndexMetadata.Builder imdBuilder = IndexMetadata.builder(leaderIndex);
        // Adding the leader index uuid for each shard as custom metadata:
        Map<String, String> customMetadata = new HashMap<>();
        customMetadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS, String.join(",", leaderHistoryUUIDs));
        customMetadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY, leaderIndexMetadata.getIndexUUID());
        customMetadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY, leaderIndexMetadata.getIndex().getName());
        customMetadata.put(Ccr.CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY, remoteClusterAlias);
        imdBuilder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, customMetadata);

        imdBuilder.settings(leaderIndexMetadata.getSettings());

        // Copy mappings from leader IMD to follow IMD
        imdBuilder.putMapping(leaderIndexMetadata.mapping());
        imdBuilder.setRoutingNumShards(leaderIndexMetadata.getRoutingNumShards());
        // We assert that insync allocation ids are not empty in `PrimaryShardAllocator`
        for (var key : leaderIndexMetadata.getInSyncAllocationIds().keySet()) {
            imdBuilder.putInSyncAllocationIds(key, Collections.singleton(IN_SYNC_ALLOCATION_ID));
        }

        return imdBuilder.build();
    }

    @Override
    public void getRepositoryData(Executor responseExecutor, ActionListener<RepositoryData> listener) {
        try {
            csDeduplicator.execute(new ThreadedActionListener<>(responseExecutor, listener.map(response -> {
                final Metadata remoteMetadata = response.getMetadata();
                final String[] concreteAllIndices = remoteMetadata.getProject().getConcreteAllIndices();
                final Map<String, SnapshotId> copiedSnapshotIds = Maps.newMapWithExpectedSize(concreteAllIndices.length);
                final Map<String, RepositoryData.SnapshotDetails> snapshotsDetails = Maps.newMapWithExpectedSize(concreteAllIndices.length);
                final Map<IndexId, List<SnapshotId>> indexSnapshots = Maps.newMapWithExpectedSize(concreteAllIndices.length);
                final Map<String, IndexMetadata> remoteIndices = remoteMetadata.getProject().indices();
                for (String indexName : concreteAllIndices) {
                    // Both the Snapshot name and UUID are set to _latest_
                    final SnapshotId snapshotId = new SnapshotId(LATEST, LATEST);
                    copiedSnapshotIds.put(indexName, snapshotId);
                    final long nowMillis = threadPool.absoluteTimeInMillis();
                    snapshotsDetails.put(
                        indexName,
                        new RepositoryData.SnapshotDetails(SnapshotState.SUCCESS, IndexVersion.current(), nowMillis, nowMillis, "")
                    );
                    indexSnapshots.put(new IndexId(indexName, remoteIndices.get(indexName).getIndex().getUUID()), List.of(snapshotId));
                }
                return new RepositoryData(
                    MISSING_UUID,
                    1,
                    copiedSnapshotIds,
                    snapshotsDetails,
                    indexSnapshots,
                    ShardGenerations.EMPTY,
                    IndexMetaDataGenerations.EMPTY,
                    MISSING_UUID
                );
            })));
        } catch (Exception e) {
            assert false;
            listener.onFailure(e);
        }
    }

    @Override
    public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryDataGeneration,
        IndexVersion minimumNodeVersion,
        ActionListener<RepositoryData> repositoryDataUpdateListener,
        Runnable onCompletion
    ) {
        repositoryDataUpdateListener.onFailure(new UnsupportedOperationException("Unsupported for repository of type: " + TYPE));
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return throttledTime.count();
    }

    @Override
    public String startVerification() {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void endVerification(String verificationToken) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {}

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void snapshotShard(SnapshotShardContext context) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void restoreShard(
        Store store,
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId snapshotShardId,
        RecoveryState recoveryState,
        ActionListener<Void> listener
    ) {
        final ShardId shardId = store.shardId();
        final LinkedList<Closeable> toClose = new LinkedList<>();
        ActionListener.run(listener, restoreShardListener -> {
            final ActionListener<Void> restoreListener = ActionListener.runBefore(
                restoreShardListener.delegateResponse(
                    (l, e) -> l.onFailure(
                        new IndexShardRestoreFailedException(shardId, "failed to restore snapshot [" + snapshotId + "]", e)
                    )
                ),
                () -> IOUtils.close(toClose)
            );
            // TODO: Add timeouts to network calls / the restore process.
            createEmptyStore(store);

            final Map<String, String> ccrMetadata = store.indexSettings().getIndexMetadata().getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
            final String leaderIndexName = ccrMetadata.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY);
            final String leaderUUID = ccrMetadata.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
            final Index leaderIndex = new Index(leaderIndexName, leaderUUID);
            final ShardId leaderShardId = new ShardId(leaderIndex, shardId.getId());

            final var remoteClient = getRemoteClusterClient();

            final String retentionLeaseId = retentionLeaseId(localClusterName, shardId.getIndex(), remoteClusterAlias, leaderIndex);

            acquireRetentionLeaseOnLeader(shardId, retentionLeaseId, leaderShardId, remoteClient);

            // schedule renewals to run during the restore
            final Scheduler.Cancellable renewable = threadPool.scheduleWithFixedDelay(() -> {
                logger.trace("{} background renewal of retention lease [{}] during restore", shardId, retentionLeaseId);
                try (var ignore = threadPool.getThreadContext().newEmptySystemContext()) {
                    // we have to execute under the system context so that if security is enabled the renewal is authorized
                    CcrRetentionLeases.asyncRenewRetentionLease(
                        leaderShardId,
                        retentionLeaseId,
                        RETAIN_ALL,
                        remoteClient,
                        ActionListener.wrap(r -> {}, e -> {
                            final Throwable cause = ExceptionsHelper.unwrapCause(e);
                            assert cause instanceof ElasticsearchSecurityException == false : cause;
                            if (cause instanceof RetentionLeaseInvalidRetainingSeqNoException == false) {
                                logger.warn(
                                    () -> format(
                                        "%s background renewal of retention lease [%s] failed during restore",
                                        shardId,
                                        retentionLeaseId
                                    ),
                                    cause
                                );
                            }
                        })
                    );
                }
            },
                CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.get(store.indexSettings().getNodeSettings()),
                remoteClientResponseExecutor
            );
            toClose.add(() -> {
                logger.trace("{} canceling background renewal of retention lease [{}] at the end of restore", shardId, retentionLeaseId);
                renewable.cancel();
            });
            // TODO: There should be some local timeout. And if the remote cluster returns an unknown session
            // response, we should be able to retry by creating a new session.
            ActionListener<RestoreSession> sessionListener = restoreListener.delegateFailureAndWrap(
                // Some tests depend on closing session before cancelling retention lease renewal.
                (l1, restoreSession) -> restoreSession.restoreFiles(store, new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.trace("[{}] completed CCR restore", shardId);
                        updateMappings(remoteClient, leaderIndex, restoreSession.mappingVersion, client, shardId.getIndex());
                        restoreSession.close(l1);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        restoreSession.close(ActionListener.running(() -> l1.onFailure(e)));
                    }
                })
            );
            openSession(metadata.name(), remoteClient, leaderShardId, shardId, recoveryState, sessionListener);
        });
    }

    private static void createEmptyStore(Store store) {
        store.incRef();
        try {
            store.createEmpty();
        } catch (final EngineException | IOException e) {
            throw new IndexShardRecoveryException(store.shardId(), "failed to create empty store", e);
        } finally {
            store.decRef();
        }
    }

    void acquireRetentionLeaseOnLeader(
        final ShardId shardId,
        final String retentionLeaseId,
        final ShardId leaderShardId,
        final RemoteClusterClient remoteClient
    ) {
        logger.trace(() -> format("%s requesting leader to add retention lease [%s]", shardId, retentionLeaseId));
        final TimeValue timeout = ccrSettings.getRecoveryActionTimeout();
        final Optional<RetentionLeaseAlreadyExistsException> maybeAddAlready = syncAddRetentionLease(
            leaderShardId,
            retentionLeaseId,
            RETAIN_ALL,
            remoteClient,
            timeout
        );
        maybeAddAlready.ifPresent(addAlready -> {
            logger.trace(
                () -> format("%s retention lease [%s] already exists, requesting a renewal", shardId, retentionLeaseId),
                addAlready
            );
            final Optional<RetentionLeaseNotFoundException> maybeRenewNotFound = syncRenewRetentionLease(
                leaderShardId,
                retentionLeaseId,
                RETAIN_ALL,
                remoteClient,
                timeout
            );
            maybeRenewNotFound.ifPresent(renewNotFound -> {
                logger.trace(
                    () -> format(
                        "%s retention lease [%s] not found while attempting to renew, requesting a final add",
                        shardId,
                        retentionLeaseId
                    ),
                    renewNotFound
                );
                final Optional<RetentionLeaseAlreadyExistsException> maybeFallbackAddAlready = syncAddRetentionLease(
                    leaderShardId,
                    retentionLeaseId,
                    RETAIN_ALL,
                    remoteClient,
                    timeout
                );
                maybeFallbackAddAlready.ifPresent(fallbackAddAlready -> {
                    /*
                     * At this point we tried to add the lease and the retention lease already existed. By the time we tried to renew the
                     * lease, it expired or was removed. We tried to add the lease again and it already exists? Bail.
                     */
                    assert false : fallbackAddAlready;
                    throw fallbackAddAlready;
                });
            });
        });
    }

    private static final ShardGeneration DUMMY_GENERATION = new ShardGeneration("");

    @Override
    public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId index, ShardId shardId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        final String leaderIndex = index.getName();
        final IndicesStatsResponse response = executeRecoveryAction(
            getRemoteClusterClient(),
            IndicesStatsAction.REMOTE_TYPE,
            new IndicesStatsRequest().indices(leaderIndex).clear().store(true)
        );
        for (ShardStats shardStats : response.getIndex(leaderIndex).getShards()) {
            final ShardRouting shardRouting = shardStats.getShardRouting();
            if (shardRouting.shardId().id() == shardId.getId() && shardRouting.primary() && shardRouting.active()) {
                // we only care about the shard size here for shard allocation, populate the rest with dummy values
                final long totalSize = shardStats.getStats().getStore().sizeInBytes();
                return IndexShardSnapshotStatus.newDone(0L, 0L, 1, 1, totalSize, totalSize, DUMMY_GENERATION);
            }
        }
        throw new ElasticsearchException("Could not get shard stats for primary of index " + leaderIndex + " on leader cluster");
    }

    @Override
    public void updateState(ClusterState state) {}

    @Override
    public void cloneShardSnapshot(
        SnapshotId source,
        SnapshotId target,
        RepositoryShardId shardId,
        ShardGeneration shardGeneration,
        ActionListener<ShardSnapshotResult> listener
    ) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void awaitIdle() {}

    private void updateMappings(
        RemoteClusterClient leaderClient,
        Index leaderIndex,
        long leaderMappingVersion,
        Client followerClient,
        Index followerIndex
    ) {
        // todo: this could manifest in production and seems we could make this async easily.
        final PlainActionFuture<IndexMetadata> indexMetadataFuture = new UnsafePlainActionFuture<>(
            Ccr.CCR_THREAD_POOL_NAME,
            ThreadPool.Names.GENERIC
        );
        final long startTimeInNanos = System.nanoTime();
        final Supplier<TimeValue> timeout = () -> {
            final long elapsedInNanos = System.nanoTime() - startTimeInNanos;
            return TimeValue.timeValueNanos(ccrSettings.getRecoveryActionTimeout().nanos() - elapsedInNanos);
        };
        CcrRequests.getIndexMetadata(leaderClient, leaderIndex, leaderMappingVersion, 0L, timeout, indexMetadataFuture);
        final IndexMetadata leaderIndexMetadata = indexMetadataFuture.actionGet(ccrSettings.getRecoveryActionTimeout());
        final MappingMetadata mappingMetadata = leaderIndexMetadata.mapping();
        if (mappingMetadata != null) {
            final PutMappingRequest putMappingRequest = CcrRequests.putMappingRequest(followerIndex.getName(), mappingMetadata);
            followerClient.admin().indices().putMapping(putMappingRequest).actionGet(ccrSettings.getRecoveryActionTimeout());
        }
    }

    void openSession(
        String repositoryName,
        RemoteClusterClient remoteClient,
        ShardId leaderShardId,
        ShardId indexShardId,
        RecoveryState recoveryState,
        ActionListener<RestoreSession> listener
    ) {
        String sessionUUID = UUIDs.randomBase64UUID();
        ActionListener<PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse> responseListener = listener.map(
            response -> new RestoreSession(
                repositoryName,
                client.getRemoteClusterClient(
                    remoteClusterAlias,
                    chunkResponseExecutor,
                    RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
                ),
                sessionUUID,
                response.getNode(),
                indexShardId,
                recoveryState,
                response.getStoreFileMetadata(),
                response.getMappingVersion(),
                threadPool,
                chunkResponseExecutor,
                ccrSettings,
                throttledTime::inc,
                leaderShardId
            )
        );
        remoteClient.execute(
            PutCcrRestoreSessionAction.REMOTE_INTERNAL_TYPE,
            new PutCcrRestoreSessionRequest(sessionUUID, leaderShardId),
            ListenerTimeouts.wrapWithTimeout(
                threadPool,
                responseListener,
                ccrSettings.getRecoveryActionTimeout(),
                threadPool.generic(), // TODO should be the remote-client response executor to match the non-timeout case
                PutCcrRestoreSessionAction.INTERNAL_NAME
            )
        );
    }

    private static class RestoreSession extends FileRestoreContext {

        private final RemoteClusterClient remoteClient;
        private final String sessionUUID;
        private final DiscoveryNode node;
        private final Store.MetadataSnapshot sourceMetadata;
        private final long mappingVersion;
        private final CcrSettings ccrSettings;
        private final LongConsumer throttleListener;
        private final ThreadPool threadPool;
        private final Executor timeoutExecutor;
        private final ShardId leaderShardId;

        RestoreSession(
            String repositoryName,
            RemoteClusterClient remoteClient,
            String sessionUUID,
            DiscoveryNode node,
            ShardId shardId,
            RecoveryState recoveryState,
            Store.MetadataSnapshot sourceMetadata,
            long mappingVersion,
            ThreadPool threadPool,
            Executor timeoutExecutor,
            CcrSettings ccrSettings,
            LongConsumer throttleListener,
            ShardId leaderShardId
        ) {
            super(repositoryName, shardId, SNAPSHOT_ID, recoveryState);
            this.remoteClient = remoteClient;
            this.sessionUUID = sessionUUID;
            this.node = node;
            this.sourceMetadata = sourceMetadata;
            this.mappingVersion = mappingVersion;
            this.threadPool = threadPool;
            this.timeoutExecutor = timeoutExecutor;
            this.ccrSettings = ccrSettings;
            this.throttleListener = throttleListener;
            this.leaderShardId = leaderShardId;

        }

        void restoreFiles(Store store, ActionListener<Void> listener) {
            ArrayList<FileInfo> fileInfos = new ArrayList<>();
            for (StoreFileMetadata fileMetadata : sourceMetadata) {
                ByteSizeValue fileSize = ByteSizeValue.ofBytes(fileMetadata.length());
                fileInfos.add(new FileInfo(fileMetadata.name(), fileMetadata, fileSize));
            }
            SnapshotFiles snapshotFiles = new SnapshotFiles(LATEST, fileInfos, null);
            restore(snapshotFiles, store, listener);
        }

        @Override
        protected void restoreFiles(List<FileInfo> filesToRecover, Store store, ActionListener<Void> allFilesListener) {
            logger.trace("[{}] starting CCR restore of {} files", shardId, filesToRecover);
            final List<StoreFileMetadata> mds = filesToRecover.stream().map(FileInfo::metadata).collect(Collectors.toList());
            final MultiChunkTransfer<StoreFileMetadata, FileChunk> multiFileTransfer = new MultiChunkTransfer<>(
                logger,
                threadPool.getThreadContext(),
                allFilesListener,
                ccrSettings.getMaxConcurrentFileChunks(),
                mds
            ) {

                final MultiFileWriter multiFileWriter = new MultiFileWriter(store, recoveryState.getIndex(), "", logger);
                long offset = 0;

                @Override
                protected void onNewResource(StoreFileMetadata md) {
                    offset = 0;
                }

                @Override
                protected FileChunk nextChunkRequest(StoreFileMetadata md) {
                    final int bytesRequested = Math.toIntExact(Math.min(ccrSettings.getChunkSize().getBytes(), md.length() - offset));
                    offset += bytesRequested;
                    return new FileChunk(md, bytesRequested, offset == md.length());
                }

                @Override
                protected void executeChunkRequest(FileChunk request, ActionListener<Void> listener) {
                    remoteClient.execute(
                        GetCcrRestoreFileChunkAction.REMOTE_INTERNAL_TYPE,
                        new GetCcrRestoreFileChunkRequest(node, sessionUUID, request.md.name(), request.bytesRequested, leaderShardId),
                        ListenerTimeouts.wrapWithTimeout(threadPool, listener.map(getCcrRestoreFileChunkResponse -> {
                            writeFileChunk(request.md, getCcrRestoreFileChunkResponse);
                            return null;
                        }), ccrSettings.getRecoveryActionTimeout(), timeoutExecutor, GetCcrRestoreFileChunkAction.INTERNAL_NAME)
                    );
                }

                private void writeFileChunk(StoreFileMetadata md, GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse r)
                    throws Exception {
                    assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
                    final int actualChunkSize = r.getChunk().length();
                    logger.trace(
                        "[{}] [{}] got response for file [{}], offset: {}, length: {}",
                        shardId,
                        snapshotId,
                        md.name(),
                        r.getOffset(),
                        actualChunkSize
                    );
                    final long nanosPaused = ccrSettings.getRateLimiter().maybePause(actualChunkSize);
                    throttleListener.accept(nanosPaused);
                    multiFileWriter.incRef();
                    try (Releasable ignored = multiFileWriter::decRef) {
                        final boolean lastChunk = r.getOffset() + actualChunkSize >= md.length();
                        multiFileWriter.writeFileChunk(md, r.getOffset(), r.getChunk(), lastChunk);
                    } catch (Exception e) {
                        handleError(md, e);
                        throw e;
                    }
                }

                @Override
                protected void handleError(StoreFileMetadata md, Exception e) throws Exception {
                    final IOException corruptIndexException;
                    if ((corruptIndexException = ExceptionsHelper.unwrapCorruption(e)) != null) {
                        try {
                            store.markStoreCorrupted(corruptIndexException);
                        } catch (IOException ioe) {
                            logger.warn("store cannot be marked as corrupted", e);
                        }
                        throw corruptIndexException;
                    }
                    throw e;
                }

                @Override
                public void close() {
                    multiFileWriter.close();
                }
            };
            multiFileTransfer.start();
        }

        public void close(ActionListener<Void> listener) {
            var closeListener = ListenerTimeouts.wrapWithTimeout(
                threadPool,
                listener,
                ccrSettings.getRecoveryActionTimeout(),
                timeoutExecutor,
                ClearCcrRestoreSessionAction.INTERNAL_NAME
            );
            ClearCcrRestoreSessionRequest clearRequest = new ClearCcrRestoreSessionRequest(sessionUUID, node, leaderShardId);
            remoteClient.execute(ClearCcrRestoreSessionAction.REMOTE_INTERNAL_TYPE, clearRequest, closeListener.map(empty -> null));
        }

        private record FileChunk(StoreFileMetadata md, int bytesRequested, boolean lastChunk) implements MultiChunkTransfer.ChunkRequest {}
    }
}
