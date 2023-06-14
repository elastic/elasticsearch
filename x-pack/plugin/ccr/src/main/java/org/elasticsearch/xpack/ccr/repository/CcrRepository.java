/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.SingleResultDeduplicator;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
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
import org.elasticsearch.repositories.GetSnapshotInfoContext;
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
import org.elasticsearch.snapshots.SnapshotDeleteListener;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
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

    private final RepositoryMetadata metadata;
    private final CcrSettings ccrSettings;
    private final String localClusterName;
    private final String remoteClusterAlias;
    private final Client client;
    private final ThreadPool threadPool;

    private final CounterMetric throttledTime = new CounterMetric();

    private final SingleResultDeduplicator<ClusterState> csDeduplicator;

    public CcrRepository(RepositoryMetadata metadata, Client client, Settings settings, CcrSettings ccrSettings, ThreadPool threadPool) {
        this.metadata = metadata;
        this.ccrSettings = ccrSettings;
        this.localClusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
        assert metadata.name().startsWith(NAME_PREFIX) : "CcrRepository metadata.name() must start with: " + NAME_PREFIX;
        this.remoteClusterAlias = Strings.split(metadata.name(), NAME_PREFIX)[1];
        this.client = client;
        this.threadPool = threadPool;
        csDeduplicator = new SingleResultDeduplicator<>(
            threadPool.getThreadContext(),
            l -> getRemoteClusterClient().admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetadata(true)
                .setNodes(true)
                .setMasterNodeTimeout(TimeValue.MAX_VALUE)
                .execute(l.map(ClusterStateResponse::getState))
        );
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }

    @Override
    public RepositoryMetadata getMetadata() {
        return metadata;
    }

    private Client getRemoteClusterClient() {
        return client.getRemoteClusterClient(remoteClusterAlias);
    }

    @Override
    public void getSnapshotInfo(GetSnapshotInfoContext context) {
        final List<SnapshotId> snapshotIds = context.snapshotIds();
        assert snapshotIds.size() == 1 && SNAPSHOT_ID.equals(snapshotIds.iterator().next())
            : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId but saw " + snapshotIds;
        try {
            csDeduplicator.execute(
                new ThreadedActionListener<>(threadPool.executor(ThreadPool.Names.SNAPSHOT_META), context.map(response -> {
                    Metadata responseMetadata = response.metadata();
                    Map<String, IndexMetadata> indicesMap = responseMetadata.indices();
                    return new SnapshotInfo(
                        new Snapshot(this.metadata.name(), SNAPSHOT_ID),
                        List.copyOf(indicesMap.keySet()),
                        List.copyOf(responseMetadata.dataStreams().keySet()),
                        List.of(),
                        response.getNodes().getMaxNodeVersion(),
                        SnapshotState.SUCCESS
                    );
                }))
            );
        } catch (Exception e) {
            assert false : e;
            context.onFailure(e);
        }
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        Client remoteClient = getRemoteClusterClient();
        // We set a single dummy index name to avoid fetching all the index data
        ClusterStateResponse clusterState = remoteClient.admin()
            .cluster()
            .state(CcrRequests.metadataRequest("dummy_index_name"))
            .actionGet(ccrSettings.getRecoveryActionTimeout());
        return clusterState.getState().metadata();
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        String leaderIndex = index.getName();
        Client remoteClient = getRemoteClusterClient();

        ClusterStateResponse clusterState = remoteClient.admin()
            .cluster()
            .state(CcrRequests.metadataRequest(leaderIndex))
            .actionGet(ccrSettings.getRecoveryActionTimeout());

        // Validates whether the leader cluster has been configured properly:
        PlainActionFuture<String[]> future = PlainActionFuture.newFuture();
        IndexMetadata leaderIndexMetadata = clusterState.getState().metadata().index(leaderIndex);
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
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        try {
            csDeduplicator.execute(listener.map(response -> {
                final Metadata remoteMetadata = response.getMetadata();
                final String[] concreteAllIndices = remoteMetadata.getConcreteAllIndices();
                final Map<String, SnapshotId> copiedSnapshotIds = Maps.newMapWithExpectedSize(concreteAllIndices.length);
                final Map<String, RepositoryData.SnapshotDetails> snapshotsDetails = Maps.newMapWithExpectedSize(concreteAllIndices.length);
                final Map<IndexId, List<SnapshotId>> indexSnapshots = Maps.newMapWithExpectedSize(concreteAllIndices.length);
                final Map<String, IndexMetadata> remoteIndices = remoteMetadata.getIndices();
                for (String indexName : concreteAllIndices) {
                    // Both the Snapshot name and UUID are set to _latest_
                    final SnapshotId snapshotId = new SnapshotId(LATEST, LATEST);
                    copiedSnapshotIds.put(indexName, snapshotId);
                    final long nowMillis = threadPool.absoluteTimeInMillis();
                    snapshotsDetails.put(
                        indexName,
                        new RepositoryData.SnapshotDetails(SnapshotState.SUCCESS, Version.CURRENT, nowMillis, nowMillis, "")
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
            }));
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
        long repositoryStateId,
        Version repositoryMetaVersion,
        SnapshotDeleteListener listener
    ) {
        listener.onFailure(new UnsupportedOperationException("Unsupported for repository of type: " + TYPE));
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
        final ActionListener<Void> restoreListener = ActionListener.runBefore(
            listener.delegateResponse(
                (l, e) -> l.onFailure(new IndexShardRestoreFailedException(shardId, "failed to restore snapshot [" + snapshotId + "]", e))
            ),
            () -> IOUtils.close(toClose)
        );
        try {
            // TODO: Add timeouts to network calls / the restore process.
            createEmptyStore(store);

            final Map<String, String> ccrMetadata = store.indexSettings().getIndexMetadata().getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
            final String leaderIndexName = ccrMetadata.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY);
            final String leaderUUID = ccrMetadata.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
            final Index leaderIndex = new Index(leaderIndexName, leaderUUID);
            final ShardId leaderShardId = new ShardId(leaderIndex, shardId.getId());

            final Client remoteClient = getRemoteClusterClient();

            final String retentionLeaseId = retentionLeaseId(localClusterName, shardId.getIndex(), remoteClusterAlias, leaderIndex);

            acquireRetentionLeaseOnLeader(shardId, retentionLeaseId, leaderShardId, remoteClient);

            // schedule renewals to run during the restore
            final Scheduler.Cancellable renewable = threadPool.scheduleWithFixedDelay(() -> {
                logger.trace("{} background renewal of retention lease [{}] during restore", shardId, retentionLeaseId);
                final ThreadContext threadContext = threadPool.getThreadContext();
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    // we have to execute under the system context so that if security is enabled the renewal is authorized
                    threadContext.markAsSystemContext();
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
                Ccr.CCR_THREAD_POOL_NAME
            );
            toClose.add(() -> {
                logger.trace("{} canceling background renewal of retention lease [{}] at the end of restore", shardId, retentionLeaseId);
                renewable.cancel();
            });
            // TODO: There should be some local timeout. And if the remote cluster returns an unknown session
            // response, we should be able to retry by creating a new session.
            final RestoreSession restoreSession = openSession(metadata.name(), remoteClient, leaderShardId, shardId, recoveryState);
            toClose.addFirst(restoreSession); // Some tests depend on closing session before cancelling retention lease renewal
            restoreSession.restoreFiles(store, restoreListener.delegateFailureAndWrap((l, v) -> {
                logger.trace("[{}] completed CCR restore", shardId);
                updateMappings(remoteClient, leaderIndex, restoreSession.mappingVersion, client, shardId.getIndex());
                l.onResponse(null);
            }));
        } catch (Exception e) {
            restoreListener.onFailure(e);
        }
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
        final Client remoteClient
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
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId index, ShardId shardId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        final String leaderIndex = index.getName();
        final IndicesStatsResponse response = getRemoteClusterClient().admin()
            .indices()
            .prepareStats(leaderIndex)
            .clear()
            .setStore(true)
            .get(ccrSettings.getRecoveryActionTimeout());
        for (ShardStats shardStats : response.getIndex(leaderIndex).getShards()) {
            final ShardRouting shardRouting = shardStats.getShardRouting();
            if (shardRouting.shardId().id() == shardId.getId() && shardRouting.primary() && shardRouting.active()) {
                // we only care about the shard size here for shard allocation, populate the rest with dummy values
                final long totalSize = shardStats.getStats().getStore().getSizeInBytes();
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
        Client leaderClient,
        Index leaderIndex,
        long leaderMappingVersion,
        Client followerClient,
        Index followerIndex
    ) {
        final PlainActionFuture<IndexMetadata> indexMetadataFuture = new PlainActionFuture<>();
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

    RestoreSession openSession(
        String repositoryName,
        Client remoteClient,
        ShardId leaderShardId,
        ShardId indexShardId,
        RecoveryState recoveryState
    ) {
        String sessionUUID = UUIDs.randomBase64UUID();
        PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse response = remoteClient.execute(
            PutCcrRestoreSessionAction.INTERNAL_INSTANCE,
            new PutCcrRestoreSessionRequest(sessionUUID, leaderShardId)
        ).actionGet(ccrSettings.getRecoveryActionTimeout());
        return new RestoreSession(
            repositoryName,
            remoteClient,
            sessionUUID,
            response.getNode(),
            indexShardId,
            recoveryState,
            response.getStoreFileMetadata(),
            response.getMappingVersion(),
            threadPool,
            ccrSettings,
            throttledTime::inc,
            leaderShardId
        );
    }

    private static class RestoreSession extends FileRestoreContext implements Closeable {

        private final Client remoteClient;
        private final String sessionUUID;
        private final DiscoveryNode node;
        private final Store.MetadataSnapshot sourceMetadata;
        private final long mappingVersion;
        private final CcrSettings ccrSettings;
        private final LongConsumer throttleListener;
        private final ThreadPool threadPool;
        private final ShardId leaderShardId;

        RestoreSession(
            String repositoryName,
            Client remoteClient,
            String sessionUUID,
            DiscoveryNode node,
            ShardId shardId,
            RecoveryState recoveryState,
            Store.MetadataSnapshot sourceMetadata,
            long mappingVersion,
            ThreadPool threadPool,
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

                final MultiFileWriter multiFileWriter = new MultiFileWriter(store, recoveryState.getIndex(), "", logger, () -> {});
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
                        GetCcrRestoreFileChunkAction.INTERNAL_INSTANCE,
                        new GetCcrRestoreFileChunkRequest(node, sessionUUID, request.md.name(), request.bytesRequested, leaderShardId),
                        ListenerTimeouts.wrapWithTimeout(threadPool, new ActionListener<>() {
                            @Override
                            public void onResponse(
                                GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse getCcrRestoreFileChunkResponse
                            ) {
                                getCcrRestoreFileChunkResponse.incRef();
                                threadPool.generic().execute(new ActionRunnable<>(listener) {
                                    @Override
                                    protected void doRun() throws Exception {
                                        writeFileChunk(request.md, getCcrRestoreFileChunkResponse);
                                        listener.onResponse(null);
                                    }

                                    @Override
                                    public void onAfter() {
                                        getCcrRestoreFileChunkResponse.decRef();
                                    }
                                });
                            }

                            @Override
                            public void onFailure(Exception e) {
                                threadPool.generic().execute(() -> {
                                    try {
                                        listener.onFailure(e);
                                    } catch (Exception ex) {
                                        e.addSuppressed(ex);
                                        logger.warn("failed to execute failure callback for chunk request", e);
                                    }
                                });
                            }
                        }, ccrSettings.getRecoveryActionTimeout(), ThreadPool.Names.GENERIC, GetCcrRestoreFileChunkAction.INTERNAL_NAME)
                    );
                }

                private void writeFileChunk(StoreFileMetadata md, GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse r)
                    throws Exception {
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

        @Override
        public void close() {
            ClearCcrRestoreSessionRequest clearRequest = new ClearCcrRestoreSessionRequest(sessionUUID, node, leaderShardId);
            ActionResponse.Empty response = remoteClient.execute(ClearCcrRestoreSessionAction.INTERNAL_INSTANCE, clearRequest)
                .actionGet(ccrSettings.getRecoveryActionTimeout());
        }

        private record FileChunk(StoreFileMetadata md, int bytesRequested, boolean lastChunk) implements MultiChunkTransfer.ChunkRequest {}
    }
}
