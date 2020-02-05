/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.mapper.MapperService;
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
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.recovery.MultiFileTransfer;
import org.elasticsearch.indices.recovery.MultiFileWriter;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.FileRestoreContext;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;
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

    private final RepositoryMetaData metadata;
    private final CcrSettings ccrSettings;
    private final String localClusterName;
    private final String remoteClusterAlias;
    private final Client client;
    private final CcrLicenseChecker ccrLicenseChecker;
    private final ThreadPool threadPool;

    private final CounterMetric throttledTime = new CounterMetric();

    public CcrRepository(RepositoryMetaData metadata, Client client, CcrLicenseChecker ccrLicenseChecker, Settings settings,
                         CcrSettings ccrSettings, ThreadPool threadPool) {
        this.metadata = metadata;
        this.ccrSettings = ccrSettings;
        this.localClusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
        assert metadata.name().startsWith(NAME_PREFIX) : "CcrRepository metadata.name() must start with: " + NAME_PREFIX;
        this.remoteClusterAlias = Strings.split(metadata.name(), NAME_PREFIX)[1];
        this.ccrLicenseChecker = ccrLicenseChecker;
        this.client = client;
        this.threadPool = threadPool;
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
    public RepositoryMetaData getMetadata() {
        return metadata;
    }

    private Client getRemoteClusterClient() {
        return client.getRemoteClusterClient(remoteClusterAlias);
    }

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        Client remoteClient = getRemoteClusterClient();
        ClusterStateResponse response = remoteClient.admin().cluster().prepareState().clear().setMetaData(true).setNodes(true)
            .get(ccrSettings.getRecoveryActionTimeout());
        ImmutableOpenMap<String, IndexMetaData> indicesMap = response.getState().metaData().indices();
        ArrayList<String> indices = new ArrayList<>(indicesMap.size());
        indicesMap.keysIt().forEachRemaining(indices::add);

        return new SnapshotInfo(snapshotId, indices, SnapshotState.SUCCESS, response.getState().getNodes().getMaxNodeVersion());
    }

    @Override
    public MetaData getSnapshotGlobalMetaData(SnapshotId snapshotId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        Client remoteClient = getRemoteClusterClient();
        // We set a single dummy index name to avoid fetching all the index data
        ClusterStateRequest clusterStateRequest = CcrRequests.metaDataRequest("dummy_index_name");
        ClusterStateResponse clusterState = remoteClient.admin().cluster().state(clusterStateRequest)
            .actionGet(ccrSettings.getRecoveryActionTimeout());
        return clusterState.getState().metaData();
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        String leaderIndex = index.getName();
        Client remoteClient = getRemoteClusterClient();

        ClusterStateRequest clusterStateRequest = CcrRequests.metaDataRequest(leaderIndex);
        ClusterStateResponse clusterState = remoteClient.admin().cluster().state(clusterStateRequest)
            .actionGet(ccrSettings.getRecoveryActionTimeout());

        // Validates whether the leader cluster has been configured properly:
        PlainActionFuture<String[]> future = PlainActionFuture.newFuture();
        IndexMetaData leaderIndexMetaData = clusterState.getState().metaData().index(leaderIndex);
        ccrLicenseChecker.fetchLeaderHistoryUUIDs(remoteClient, leaderIndexMetaData, future::onFailure, future::onResponse);
        String[] leaderHistoryUUIDs = future.actionGet(ccrSettings.getRecoveryActionTimeout());

        IndexMetaData.Builder imdBuilder = IndexMetaData.builder(leaderIndex);
        // Adding the leader index uuid for each shard as custom metadata:
        Map<String, String> metadata = new HashMap<>();
        metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS, String.join(",", leaderHistoryUUIDs));
        metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY, leaderIndexMetaData.getIndexUUID());
        metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY, leaderIndexMetaData.getIndex().getName());
        metadata.put(Ccr.CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY, remoteClusterAlias);
        imdBuilder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, metadata);

        imdBuilder.settings(leaderIndexMetaData.getSettings());

        // Copy mappings from leader IMD to follow IMD
        imdBuilder.putMapping(leaderIndexMetaData.mapping());
        imdBuilder.setRoutingNumShards(leaderIndexMetaData.getRoutingNumShards());
        // We assert that insync allocation ids are not empty in `PrimaryShardAllocator`
        for (IntObjectCursor<Set<String>> entry : leaderIndexMetaData.getInSyncAllocationIds()) {
            imdBuilder.putInSyncAllocationIds(entry.key, Collections.singleton(IN_SYNC_ALLOCATION_ID));
        }

        return imdBuilder.build();
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        ActionListener.completeWith(listener, () -> {
            Client remoteClient = getRemoteClusterClient();
            ClusterStateResponse response = remoteClient.admin().cluster().prepareState().clear().setMetaData(true)
                .get(ccrSettings.getRecoveryActionTimeout());
            MetaData remoteMetaData = response.getState().getMetaData();

            Map<String, SnapshotId> copiedSnapshotIds = new HashMap<>();
            Map<String, SnapshotState> snapshotStates = new HashMap<>(copiedSnapshotIds.size());
            Map<String, Version> snapshotVersions = new HashMap<>(copiedSnapshotIds.size());
            Map<IndexId, Set<SnapshotId>> indexSnapshots = new HashMap<>(copiedSnapshotIds.size());

            ImmutableOpenMap<String, IndexMetaData> remoteIndices = remoteMetaData.getIndices();
            for (String indexName : remoteMetaData.getConcreteAllIndices()) {
                // Both the Snapshot name and UUID are set to _latest_
                SnapshotId snapshotId = new SnapshotId(LATEST, LATEST);
                copiedSnapshotIds.put(indexName, snapshotId);
                snapshotStates.put(indexName, SnapshotState.SUCCESS);
                snapshotVersions.put(indexName, Version.CURRENT);
                Index index = remoteIndices.get(indexName).getIndex();
                indexSnapshots.put(new IndexId(indexName, index.getUUID()), Collections.singleton(snapshotId));
            }
            return new RepositoryData(1, copiedSnapshotIds, snapshotStates, snapshotVersions, indexSnapshots, ShardGenerations.EMPTY);
        });
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId, ShardGenerations shardGenerations, long startTime, String failure, int totalShards,
                                 List<SnapshotShardFailure> shardFailures, long repositoryStateId, boolean includeGlobalState,
                                 MetaData metaData, Map<String, Object> userMetadata, boolean writeShardGens,
                                 ActionListener<SnapshotInfo> listener) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId, boolean writeShardGens, ActionListener<Void> listener) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
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
    public void verify(String verificationToken, DiscoveryNode localNode) {
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus, boolean writeShardGens,
                              Map<String, Object> userMetadata, ActionListener<String> listener) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void restoreShard(Store store, SnapshotId snapshotId, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState,
                             ActionListener<Void> listener) {
        final ShardId shardId = store.shardId();
        final LinkedList<Closeable> toClose = new LinkedList<>();
        final ActionListener<Void> restoreListener = ActionListener.runBefore(ActionListener.delegateResponse(listener,
            (l, e) -> l.onFailure(new IndexShardRestoreFailedException(shardId, "failed to restore snapshot [" + snapshotId + "]", e))),
            () -> IOUtils.close(toClose));
        try {
            // TODO: Add timeouts to network calls / the restore process.
            createEmptyStore(store);

            final Map<String, String> ccrMetaData = store.indexSettings().getIndexMetaData().getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
            final String leaderIndexName = ccrMetaData.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY);
            final String leaderUUID = ccrMetaData.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
            final Index leaderIndex = new Index(leaderIndexName, leaderUUID);
            final ShardId leaderShardId = new ShardId(leaderIndex, shardId.getId());

            final Client remoteClient = getRemoteClusterClient();

            final String retentionLeaseId =
                retentionLeaseId(localClusterName, shardId.getIndex(), remoteClusterAlias, leaderIndex);

            acquireRetentionLeaseOnLeader(shardId, retentionLeaseId, leaderShardId, remoteClient);

            // schedule renewals to run during the restore
            final Scheduler.Cancellable renewable = threadPool.scheduleWithFixedDelay(
                () -> {
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
                            ActionListener.wrap(
                                r -> {},
                                e -> {
                                    final Throwable cause = ExceptionsHelper.unwrapCause(e);
                                    assert cause instanceof ElasticsearchSecurityException == false : cause;
                                    if (cause instanceof RetentionLeaseInvalidRetainingSeqNoException == false) {
                                        logger.warn(new ParameterizedMessage(
                                            "{} background renewal of retention lease [{}] failed during restore", shardId,
                                            retentionLeaseId), cause);
                                    }
                                }));
                    }
                },
                CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.get(store.indexSettings().getNodeSettings()),
                Ccr.CCR_THREAD_POOL_NAME);
            toClose.add(() -> {
                logger.trace(
                    "{} canceling background renewal of retention lease [{}] at the end of restore", shardId, retentionLeaseId);
                renewable.cancel();
            });
            // TODO: There should be some local timeout. And if the remote cluster returns an unknown session
            //  response, we should be able to retry by creating a new session.
            final RestoreSession restoreSession = openSession(metadata.name(), remoteClient, leaderShardId, shardId, recoveryState);
            toClose.addFirst(restoreSession); // Some tests depend on closing session before cancelling retention lease renewal
            restoreSession.restoreFiles(store, ActionListener.wrap(v -> {
                logger.trace("[{}] completed CCR restore", shardId);
                updateMappings(remoteClient, leaderIndex, restoreSession.mappingVersion, client, shardId.getIndex());
                restoreListener.onResponse(null);
            }, restoreListener::onFailure));
        } catch (Exception e) {
            restoreListener.onFailure(e);
        }
    }

    private void createEmptyStore(Store store) {
        store.incRef();
        try {
            store.createEmpty(store.indexSettings().getIndexVersionCreated().luceneVersion);
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
            final Client remoteClient) {
        logger.trace(
                () -> new ParameterizedMessage("{} requesting leader to add retention lease [{}]", shardId, retentionLeaseId));
        final TimeValue timeout = ccrSettings.getRecoveryActionTimeout();
        final Optional<RetentionLeaseAlreadyExistsException> maybeAddAlready =
                syncAddRetentionLease(leaderShardId, retentionLeaseId, RETAIN_ALL, remoteClient, timeout);
        maybeAddAlready.ifPresent(addAlready -> {
            logger.trace(() -> new ParameterizedMessage(
                            "{} retention lease [{}] already exists, requesting a renewal",
                            shardId,
                            retentionLeaseId),
                    addAlready);
            final Optional<RetentionLeaseNotFoundException> maybeRenewNotFound =
                    syncRenewRetentionLease(leaderShardId, retentionLeaseId, RETAIN_ALL, remoteClient, timeout);
            maybeRenewNotFound.ifPresent(renewNotFound -> {
                logger.trace(() -> new ParameterizedMessage(
                                "{} retention lease [{}] not found while attempting to renew, requesting a final add",
                                shardId,
                                retentionLeaseId),
                        renewNotFound);
                final Optional<RetentionLeaseAlreadyExistsException> maybeFallbackAddAlready =
                        syncAddRetentionLease(leaderShardId, retentionLeaseId, RETAIN_ALL, remoteClient, timeout);
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

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId leaderShardId) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void updateState(ClusterState state) {
    }

    private void updateMappings(Client leaderClient, Index leaderIndex, long leaderMappingVersion,
                                Client followerClient, Index followerIndex) {
        final PlainActionFuture<IndexMetaData> indexMetadataFuture = new PlainActionFuture<>();
        final long startTimeInNanos = System.nanoTime();
        final Supplier<TimeValue> timeout = () -> {
            final long elapsedInNanos = System.nanoTime() - startTimeInNanos;
            return TimeValue.timeValueNanos(ccrSettings.getRecoveryActionTimeout().nanos() - elapsedInNanos);
        };
        CcrRequests.getIndexMetadata(leaderClient, leaderIndex, leaderMappingVersion, 0L, timeout, indexMetadataFuture);
        final IndexMetaData leaderIndexMetadata = indexMetadataFuture.actionGet(ccrSettings.getRecoveryActionTimeout());
        final MappingMetaData mappingMetaData = leaderIndexMetadata.mapping();
        if (mappingMetaData != null) {
            final PutMappingRequest putMappingRequest = CcrRequests.putMappingRequest(followerIndex.getName(), mappingMetaData)
                .masterNodeTimeout(TimeValue.timeValueMinutes(30));
            followerClient.admin().indices().putMapping(putMappingRequest).actionGet(ccrSettings.getRecoveryActionTimeout());
        }
    }

    RestoreSession openSession(String repositoryName, Client remoteClient, ShardId leaderShardId, ShardId indexShardId,
                                       RecoveryState recoveryState) {
        String sessionUUID = UUIDs.randomBase64UUID();
        PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse response = remoteClient.execute(PutCcrRestoreSessionAction.INSTANCE,
            new PutCcrRestoreSessionRequest(sessionUUID, leaderShardId)).actionGet(ccrSettings.getRecoveryActionTimeout());
        return new RestoreSession(repositoryName, remoteClient, sessionUUID, response.getNode(), indexShardId, recoveryState,
            response.getStoreFileMetaData(), response.getMappingVersion(), threadPool, ccrSettings, throttledTime::inc);
    }

    private static class RestoreSession extends FileRestoreContext implements Closeable {

        private final Client remoteClient;
        private final String sessionUUID;
        private final DiscoveryNode node;
        private final Store.MetadataSnapshot sourceMetaData;
        private final long mappingVersion;
        private final CcrSettings ccrSettings;
        private final LongConsumer throttleListener;
        private final ThreadPool threadPool;

        RestoreSession(String repositoryName, Client remoteClient, String sessionUUID, DiscoveryNode node, ShardId shardId,
                       RecoveryState recoveryState, Store.MetadataSnapshot sourceMetaData, long mappingVersion,
                       ThreadPool threadPool, CcrSettings ccrSettings, LongConsumer throttleListener) {
            super(repositoryName, shardId, SNAPSHOT_ID, recoveryState);
            this.remoteClient = remoteClient;
            this.sessionUUID = sessionUUID;
            this.node = node;
            this.sourceMetaData = sourceMetaData;
            this.mappingVersion = mappingVersion;
            this.threadPool = threadPool;
            this.ccrSettings = ccrSettings;
            this.throttleListener = throttleListener;
        }

        void restoreFiles(Store store, ActionListener<Void> listener) {
            ArrayList<FileInfo> fileInfos = new ArrayList<>();
            for (StoreFileMetaData fileMetaData : sourceMetaData) {
                ByteSizeValue fileSize = new ByteSizeValue(fileMetaData.length());
                fileInfos.add(new FileInfo(fileMetaData.name(), fileMetaData, fileSize));
            }
            SnapshotFiles snapshotFiles = new SnapshotFiles(LATEST, fileInfos);
            restore(snapshotFiles, store, listener);
        }

        @Override
        protected void restoreFiles(List<FileInfo> filesToRecover, Store store, ActionListener<Void> allFilesListener) {
            logger.trace("[{}] starting CCR restore of {} files", shardId, filesToRecover);
            final List<StoreFileMetaData> mds = filesToRecover.stream().map(FileInfo::metadata).collect(Collectors.toList());
            final MultiFileTransfer<FileChunk> multiFileTransfer = new MultiFileTransfer<>(
                logger, threadPool.getThreadContext(), allFilesListener, ccrSettings.getMaxConcurrentFileChunks(), mds) {

                final MultiFileWriter multiFileWriter = new MultiFileWriter(store, recoveryState.getIndex(), "", logger, () -> {
                });
                long offset = 0;

                @Override
                protected void onNewFile(StoreFileMetaData md) {
                    offset = 0;
                }

                @Override
                protected FileChunk nextChunkRequest(StoreFileMetaData md) {
                    final int bytesRequested = Math.toIntExact(Math.min(ccrSettings.getChunkSize().getBytes(), md.length() - offset));
                    offset += bytesRequested;
                    return new FileChunk(md, bytesRequested, offset == md.length());
                }

                @Override
                protected void executeChunkRequest(FileChunk request, ActionListener<Void> listener) {
                    final ActionListener<GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse> threadedListener
                        = new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.GENERIC, ActionListener.wrap(r -> {
                            writeFileChunk(request.md, r);
                            listener.onResponse(null);
                        }, listener::onFailure), false);

                    remoteClient.execute(GetCcrRestoreFileChunkAction.INSTANCE,
                        new GetCcrRestoreFileChunkRequest(node, sessionUUID, request.md.name(), request.bytesRequested),
                        ListenerTimeouts.wrapWithTimeout(threadPool, threadedListener, ccrSettings.getRecoveryActionTimeout(),
                            ThreadPool.Names.GENERIC, GetCcrRestoreFileChunkAction.NAME));
                }

                private void writeFileChunk(StoreFileMetaData md,
                    GetCcrRestoreFileChunkAction.GetCcrRestoreFileChunkResponse r) throws Exception {
                    final int actualChunkSize = r.getChunk().length();
                    logger.trace("[{}] [{}] got response for file [{}], offset: {}, length: {}",
                        shardId, snapshotId, md.name(), r.getOffset(), actualChunkSize);
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
                protected void handleError(StoreFileMetaData md, Exception e) throws Exception {
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
            ClearCcrRestoreSessionRequest clearRequest = new ClearCcrRestoreSessionRequest(sessionUUID, node);
            ClearCcrRestoreSessionAction.ClearCcrRestoreSessionResponse response =
                remoteClient.execute(ClearCcrRestoreSessionAction.INSTANCE, clearRequest).actionGet(ccrSettings.getRecoveryActionTimeout());
        }

        private static class FileChunk implements MultiFileTransfer.ChunkRequest {
            final StoreFileMetaData md;
            final int bytesRequested;
            final boolean lastChunk;

            FileChunk(StoreFileMetaData md, int bytesRequested, boolean lastChunk) {
                this.md = md;
                this.bytesRequested = bytesRequested;
                this.lastChunk = lastChunk;
            }

            @Override
            public boolean lastChunk() {
                return lastChunk;
            }
        }
    }
}
