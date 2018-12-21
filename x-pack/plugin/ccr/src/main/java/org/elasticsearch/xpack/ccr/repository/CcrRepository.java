/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.repository;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardRecoveryException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
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
import org.elasticsearch.xpack.ccr.action.CcrRequests;
import org.elasticsearch.xpack.ccr.action.repositories.ClearCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.action.repositories.ClearCcrRestoreSessionRequest;
import org.elasticsearch.xpack.ccr.action.repositories.PutCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.action.repositories.PutCcrRestoreSessionRequest;

import java.io.IOException;
import java.util.ArrayList;
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
        // We set a single dummy index name to avoid fetching all the index data
        ClusterStateRequest clusterStateRequest = CcrRequests.metaDataRequest("dummy_index_name");
        ClusterStateResponse clusterState = remoteClient.admin().cluster().state(clusterStateRequest).actionGet();
        return clusterState.getState().metaData();
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        String leaderIndex = index.getName();
        Client remoteClient = client.getRemoteClusterClient(remoteClusterAlias);

        ClusterStateRequest clusterStateRequest = CcrRequests.metaDataRequest(leaderIndex);
        ClusterStateResponse clusterState = remoteClient.admin().cluster().state(clusterStateRequest).actionGet();

        // Validates whether the leader cluster has been configured properly:
        PlainActionFuture<String[]> future = PlainActionFuture.newFuture();
        IndexMetaData leaderIndexMetaData = clusterState.getState().metaData().index(leaderIndex);
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
        // TODO: Add timeouts to network calls / the restore process.
        final Store store = indexShard.store();
        store.incRef();
        try {
            store.createEmpty(indexShard.indexSettings().getIndexMetaData().getCreationVersion().luceneVersion);
        } catch (EngineException | IOException e) {
            throw new IndexShardRecoveryException(shardId, "failed to create empty store", e);
        } finally {
            store.decRef();
        }

        Store.MetadataSnapshot recoveryMetadata;
        try {
            recoveryMetadata = indexShard.snapshotStoreMetadata();
        } catch (IOException e) {
            throw new IndexShardRecoveryException(shardId, "failed access store metadata", e);
        }

        Map<String, String> ccrMetaData = indexShard.indexSettings().getIndexMetaData().getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
        String leaderUUID = ccrMetaData.get(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
        Index leaderIndex = new Index(shardId.getIndexName(), leaderUUID);
        ShardId leaderShardId = new ShardId(leaderIndex, shardId.getId());

        Client remoteClient = client.getRemoteClusterClient(remoteClusterAlias);
        String sessionUUID = UUIDs.randomBase64UUID();
        PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse response = remoteClient.execute(PutCcrRestoreSessionAction.INSTANCE,
            new PutCcrRestoreSessionRequest(sessionUUID, leaderShardId, recoveryMetadata)).actionGet();
        DiscoveryNode node = response.getNode();
        // TODO: Implement file restore
        closeSession(remoteClient, node, sessionUUID);
        maybeUpdateMappings(client, remoteClient, leaderIndex, indexShard.indexSettings());
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId leaderShardId) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    private void maybeUpdateMappings(Client localClient, Client remoteClient, Index leaderIndex, IndexSettings followerIndexSettings) {
        ClusterStateRequest clusterStateRequest = CcrRequests.metaDataRequest(leaderIndex.getName());
        ClusterStateResponse clusterState = remoteClient.admin().cluster().state(clusterStateRequest).actionGet();
        IndexMetaData leaderIndexMetadata = clusterState.getState().metaData().getIndexSafe(leaderIndex);
        long leaderMappingVersion = leaderIndexMetadata.getMappingVersion();

        if (leaderMappingVersion > followerIndexSettings.getIndexMetaData().getMappingVersion()) {
            Index followerIndex = followerIndexSettings.getIndex();
            MappingMetaData mappingMetaData = leaderIndexMetadata.mapping();
            PutMappingRequest putMappingRequest = CcrRequests.putMappingRequest(followerIndex.getName(), mappingMetaData);
            localClient.admin().indices().putMapping(putMappingRequest).actionGet();
        }
    }

    private void closeSession(Client remoteClient, DiscoveryNode node, String sessionUUID) {
        ClearCcrRestoreSessionRequest clearRequest = new ClearCcrRestoreSessionRequest(sessionUUID, node);
        ClearCcrRestoreSessionAction.ClearCcrRestoreSessionResponse response =
            remoteClient.execute(ClearCcrRestoreSessionAction.INSTANCE, clearRequest).actionGet();
    }
}
