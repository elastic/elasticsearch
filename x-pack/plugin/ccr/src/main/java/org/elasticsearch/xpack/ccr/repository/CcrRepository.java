/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardRecoveryException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteCcrRestoreSessionRequest;
import org.elasticsearch.xpack.ccr.action.repositories.PutCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.action.repositories.PutCcrRestoreSessionRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This repository relies on a remote cluster for Ccr restores. It is read-only so it can only be used to
 * restore shards/indexes that exist on the remote cluster.
 */
public class CcrRepository extends AbstractLifecycleComponent implements Repository {

    private static final Logger logger = LogManager.getLogger(CcrRestoreSourceService.class);

    public static final String LATEST = "_latest_";
    public static final String TYPE = "_ccr_";
    public static final String NAME_PREFIX = "_ccr_";
    private static final SnapshotId SNAPSHOT_ID = new SnapshotId(LATEST, LATEST);

    private final RepositoryMetaData metadata;
    private final String remoteClusterAlias;
    private final Client client;
    private final CcrLicenseChecker ccrLicenseChecker;

    public CcrRepository(RepositoryMetaData metadata, Client client, CcrLicenseChecker ccrLicenseChecker, Settings settings) {
        super(settings);
        this.metadata = metadata;
        assert metadata.name().startsWith(NAME_PREFIX) : "CcrRepository metadata.name() must start with: " + NAME_PREFIX;
        this.remoteClusterAlias = Strings.split(metadata.name(), NAME_PREFIX)[1];
        this.ccrLicenseChecker = ccrLicenseChecker;
        this.client = client;
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

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        Client remoteClient = client.getRemoteClusterClient(remoteClusterAlias);
        ClusterStateResponse response = remoteClient.admin().cluster().prepareState().clear().setMetaData(true).setNodes(true).get();
        ImmutableOpenMap<String, IndexMetaData> indicesMap = response.getState().metaData().indices();
        ArrayList<String> indices = new ArrayList<>(indicesMap.size());
        indicesMap.keysIt().forEachRemaining(indices::add);

        return new SnapshotInfo(snapshotId, indices, SnapshotState.SUCCESS, response.getState().getNodes().getMaxNodeVersion());
    }

    @Override
    public MetaData getSnapshotGlobalMetaData(SnapshotId snapshotId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        Client remoteClient = client.getRemoteClusterClient(remoteClusterAlias);
        ClusterStateResponse response = remoteClient
            .admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetaData(true)
            .setIndices("dummy_index_name") // We set a single dummy index name to avoid fetching all the index data
            .get();
        return response.getState().metaData();
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        String leaderIndex = index.getName();
        Client remoteClient = client.getRemoteClusterClient(remoteClusterAlias);

        ClusterStateResponse response = remoteClient
            .admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetaData(true)
            .setIndices(leaderIndex)
            .get();

        // Validates whether the leader cluster has been configured properly:
        PlainActionFuture<String[]> future = PlainActionFuture.newFuture();
        IndexMetaData leaderIndexMetaData = response.getState().metaData().index(leaderIndex);
        ccrLicenseChecker.fetchLeaderHistoryUUIDs(remoteClient, leaderIndexMetaData, future::onFailure, future::onResponse);
        String[] leaderHistoryUUIDs = future.actionGet();

        IndexMetaData.Builder imdBuilder = IndexMetaData.builder(leaderIndexMetaData);
        // Adding the leader index uuid for each shard as custom metadata:
        Map<String, String> metadata = new HashMap<>();
        metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS, String.join(",", leaderHistoryUUIDs));
        metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY, leaderIndexMetaData.getIndexUUID());
        metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY, leaderIndexMetaData.getIndex().getName());
        metadata.put(Ccr.CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY, remoteClusterAlias);
        imdBuilder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, metadata);

        return imdBuilder.build();
    }

    @Override
    public RepositoryData getRepositoryData() {
        Client remoteClient = client.getRemoteClusterClient(remoteClusterAlias);
        ClusterStateResponse response = remoteClient.admin().cluster().prepareState().clear().setMetaData(true).get();
        MetaData remoteMetaData = response.getState().getMetaData();

        Map<String, SnapshotId> copiedSnapshotIds = new HashMap<>();
        Map<String, SnapshotState> snapshotStates = new HashMap<>(copiedSnapshotIds.size());
        Map<IndexId, Set<SnapshotId>> indexSnapshots = new HashMap<>(copiedSnapshotIds.size());

        ImmutableOpenMap<String, IndexMetaData> remoteIndices = remoteMetaData.getIndices();
        for (String indexName : remoteMetaData.getConcreteAllIndices()) {
            // Both the Snapshot name and UUID are set to _latest_
            SnapshotId snapshotId = new SnapshotId(LATEST, LATEST);
            copiedSnapshotIds.put(indexName, snapshotId);
            snapshotStates.put(indexName, SnapshotState.SUCCESS);
            Index index = remoteIndices.get(indexName).getIndex();
            indexSnapshots.put(new IndexId(indexName, index.getUUID()), Collections.singleton(snapshotId));
        }

        return new RepositoryData(1, copiedSnapshotIds, snapshotStates, indexSnapshots, Collections.emptyList());
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData metaData) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public SnapshotInfo finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure, int totalShards,
                                         List<SnapshotShardFailure> shardFailures, long repositoryStateId, boolean includeGlobalState) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return 0;
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
    public void snapshotShard(IndexShard shard, Store store, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit,
                              IndexShardSnapshotStatus snapshotStatus) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void restoreShard(IndexShard indexShard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId,
                             RecoveryState recoveryState) {
        Client remoteClient = client.getRemoteClusterClient(remoteClusterAlias);
        String sessionUUID = UUIDs.randomBase64UUID();
        Store.MetadataSnapshot recoveryTargetMetadata;
        try {
            // this will throw an IOException if the store has no segments infos file. The
            // store can still have existing files but they will be deleted just before being
            // restored.
            recoveryTargetMetadata = indexShard.snapshotStoreMetadata();
        } catch (IndexNotFoundException e) {
            // happens when restore to an empty shard, not a big deal
            logger.trace("[{}] [{}] restoring from to an empty shard", shardId, SNAPSHOT_ID);
            recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
        } catch (IOException e) {
            logger.warn("{} Can't read metadata from store, will not reuse any local file while restoring", shardId, e);
            recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
        }


        PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse response = remoteClient.execute(PutCcrRestoreSessionAction.INSTANCE,
            new PutCcrRestoreSessionRequest(sessionUUID, shardId, recoveryTargetMetadata)).actionGet();
        String nodeId = response.getNodeId();

        try {
            RestoreSession restoreSession = new RestoreSession(remoteClient, null, nodeId, indexShard, recoveryState, null);
            restoreSession.restore();
        } catch (Exception e) {
            throw new IndexShardRecoveryException(shardId, "failed to recover from gateway", e);
        } finally {
            remoteClient.execute(DeleteCcrRestoreSessionAction.INSTANCE, new DeleteCcrRestoreSessionRequest()).actionGet();
        }
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    private static class RestoreSession {

        private final int BUFFER_SIZE = 1 << 16;

        private final Client remoteClient;
        private final String sessionUUID;
        private final String nodeId;
        private final IndexShard indexShard;
        private final RecoveryState recoveryState;
        private final Store.MetadataSnapshot sourceMetaData;

        RestoreSession(Client remoteClient, String sessionUUID, String nodeId, IndexShard indexShard, RecoveryState recoveryState,
                       Store.MetadataSnapshot sourceMetaData) {
            this.remoteClient = remoteClient;
            this.sessionUUID = sessionUUID;
            this.nodeId = nodeId;
            this.indexShard = indexShard;
            this.recoveryState = recoveryState;
            this.sourceMetaData = sourceMetaData;
        }

        void restore() throws IOException {
            final Store store = indexShard.store();
            store.incRef();
            try {
                final ShardId shardId = indexShard.shardId();
                logger.debug("[{}] repository restoring shard [{}]", CcrRepository.TYPE, shardId);

                Store.MetadataSnapshot recoveryTargetMetadata;
                try {
                    // this will throw an IOException if the store has no segments infos file. The
                    // store can still have existing files but they will be deleted just before being
                    // restored.
                    recoveryTargetMetadata = indexShard.snapshotStoreMetadata();
                } catch (IndexNotFoundException e) {
                    // happens when restore to an empty shard, not a big deal
                    logger.trace("[{}] [{}] restoring from to an empty shard", shardId, SNAPSHOT_ID);
                    recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
                } catch (IOException e) {
                    logger.warn("{} Can't read metadata from store, will not reuse any local file while restoring", shardId, e);
                    recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
                }

                final StoreFileMetaData restoredSegmentsFile = sourceMetaData.getSegmentsFile();
                if (restoredSegmentsFile == null) {
                    throw new IndexShardRestoreFailedException(shardId, "Snapshot has no segments file");
                }

                final Store.RecoveryDiff diff = sourceMetaData.recoveryDiff(recoveryTargetMetadata);
                for (StoreFileMetaData fileMetaData : diff.identical) {
                    recoveryState.getIndex().addFileDetail(fileMetaData.name(), fileMetaData.length(), true);
                    logger.trace("[{}] not_recovering [{}] from [{}], exists in local store and is same", shardId, fileMetaData.name());
                }

                List<StoreFileMetaData> filesToRecover = new ArrayList<>();
                for (StoreFileMetaData fileMetaData : Iterables.concat(diff.different, diff.missing)) {
                    filesToRecover.add(fileMetaData);
                    recoveryState.getIndex().addFileDetail(fileMetaData.name(), fileMetaData.length(), false);
                    logger.trace("[{}] recovering [{}], exists in local store but is different", shardId, fileMetaData.name());
                }

                if (filesToRecover.isEmpty()) {
                    logger.trace("no files to recover, all exists within the local store");
                }

                // list of all existing store files
                final List<String> deleteIfExistFiles = Arrays.asList(store.directory().listAll());

                // restore the files from the snapshot to the Lucene store
                for (final StoreFileMetaData fileToRecover : filesToRecover) {
                    // if a file with a same physical name already exist in the store we need to delete it
                    // before restoring it from the snapshot. We could be lenient and try to reuse the existing
                    // store files (and compare their names/length/checksum again with the snapshot files) but to
                    // avoid extra complexity we simply delete them and restore them again like StoreRecovery
                    // does with dangling indices. Any existing store file that is not restored from the snapshot
                    // will be clean up by RecoveryTarget.cleanFiles().
                    final String name = fileToRecover.name();
                    if (deleteIfExistFiles.contains(name)) {
                        logger.trace("[{}] deleting pre-existing file [{}]", shardId, name);
                        store.directory().deleteFile(name);
                    }

                    logger.trace("[{}] restoring file [{}]", shardId, fileToRecover.name());
                    restoreFile(fileToRecover, store);
                }

                // read the snapshot data persisted
                final SegmentInfos segmentCommitInfos;
                try {
                    segmentCommitInfos = Lucene.pruneUnreferencedFiles(restoredSegmentsFile.name(), store.directory());
                } catch (IOException e) {
                    throw new IndexShardRestoreFailedException(shardId, "Failed to fetch index version after copying it over", e);
                }
                recoveryState.getIndex().updateVersion(segmentCommitInfos.getVersion());

                // now, go over and clean files that are in the store, but were not in the snapshot
                store.cleanupAndVerify("restore complete from remote", sourceMetaData);
            } finally {
                store.decRef();
            }
        }

        private void restoreFile(StoreFileMetaData fileToRecover, Store store) throws IOException {
//            boolean success = false;
//
//            try (InputStream stream = new RestoreFileInputStream(remoteClient, sessionUUID, nodeId, fileToRecover)) {
//                try (IndexOutput indexOutput = store.createVerifyingOutput(fileToRecover.name(), fileToRecover, IOContext.DEFAULT)) {
//                    final byte[] buffer = new byte[BUFFER_SIZE];
//                    int length;
//                    while ((length = stream.read(buffer)) > 0) {
//                        indexOutput.writeBytes(buffer, 0, length);
//                        recoveryState.getIndex().addRecoveredBytesToFile(fileToRecover.name(), length);
//                    }
//                    Store.verify(indexOutput);
//                    indexOutput.close();
//                    store.directory().sync(Collections.singleton(fileToRecover.name()));
//                    success = true;
//                } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
//                    try {
//                        store.markStoreCorrupted(ex);
//                    } catch (IOException e) {
//                        logger.warn("store cannot be marked as corrupted", e);
//                    }
//                    throw ex;
//                } finally {
//                    if (success == false) {
//                        store.deleteQuiet(fileToRecover.name());
//                    }
//                }
//            }
        }
    }

//    private static class RestoreFileInputStream extends InputStream {
//
//        private final Client remoteClient;
//        private final String sessionUUID;
//        private final String nodeId;
//        private final StoreFileMetaData fileInfo;
//
//        private long pos = 0;
//
//        private RestoreFileInputStream(Client remoteClient, String sessionUUID, String nodeId, StoreFileMetaData fileInfo) {
//            this.remoteClient = remoteClient;
//            this.sessionUUID = sessionUUID;
//            this.nodeId = nodeId;
//            this.fileInfo = fileInfo;
//        }
//
//
//        @Override
//        public int read() throws IOException {
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public int read(byte[] bytes, int off, int len) throws IOException {
//            if (pos >= fileInfo.length()) {
//                return 0;
//            }
//
//            GetCcrRestoreFileChunkRequest request = GetCcrRestoreFileChunkAction.createRequest(nodeId, sessionUUID,
//                fileInfo.name(), pos, (int) Math.min(fileInfo.length() - pos, len));
//            byte[] fileChunk = remoteClient.execute(GetCcrRestoreFileChunkAction.INSTANCE, request).actionGet().getChunk();
//
//            pos += fileChunk.length;
//            System.arraycopy(fileChunk, 0, bytes, off, fileChunk.length);
//            return fileChunk.length;
//        }
//    }
}
