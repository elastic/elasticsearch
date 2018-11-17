/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.xpack.ccr.respository;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RemoteClusterRepository extends AbstractLifecycleComponent implements Repository {

    public final static String SNAPSHOT_UUID = "_latest_";
    public final static String TYPE = "_remote_cluster_";

    private final CcrLicenseChecker ccrLicenseChecker = null;
    private final RepositoryMetaData metadata;
    private final Map<SnapshotId, Map<IndexId, IndexMetaData>> snapshotMetaData = ConcurrentCollections.newConcurrentMap();
    private final Client client;

    public RemoteClusterRepository(RepositoryMetaData metadata, Client client, Settings settings) {
        super(settings);
        this.metadata = metadata;
        this.client = client;
    }

    public synchronized void registerSnapshotMetaData(SnapshotId snapshotId, Map<IndexId, IndexMetaData> indexMetaData) {
        Map<IndexId, IndexMetaData> existingIndexMetaData = snapshotMetaData.get(snapshotId);
        if (existingIndexMetaData != null) {
            // TODO: Maybe need to validate that is does not already exist
            existingIndexMetaData.putAll(indexMetaData);
        } else {
            snapshotMetaData.put(snapshotId, new HashMap<>(indexMetaData));
        }
    }

    public void removeSnapshotMetaData(String remoteClusterAlias) {

    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public RepositoryMetaData getMetadata() {
        return metadata;
    }

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        assert SNAPSHOT_UUID.equals(snapshotId.getUUID()) : "RemoteClusterRepository only supports the _latest_ as the UUID";
        ClusterStateResponse response = client.admin().cluster().prepareState().clear().setMetaData(true).get();
        // TODO: Perhaps add version
        return new SnapshotInfo(snapshotId, Arrays.asList(response.getState().metaData().getConcreteAllIndices()), SnapshotState.SUCCESS);
    }

    @Override
    public MetaData getSnapshotGlobalMetaData(SnapshotId snapshotId) {
        assert SNAPSHOT_UUID.equals(snapshotId.getUUID()) : "RemoteClusterRepository only supports the _latest_ as the UUID";
        ClusterStateResponse response = client.admin().cluster().prepareState().clear().setMetaData(true).get();
        return response.getState().metaData();
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
        assert SNAPSHOT_UUID.equals(snapshotId.getUUID()) : "RemoteClusterRepository only supports the _latest_ as the UUID";
        String remoteCluster = snapshotId.getName();

        // Validates whether the leader cluster has been configured properly:
        Client remoteClusterClient = client.getRemoteClusterClient(remoteCluster);
        String leaderIndex = index.getName();
        PlainActionFuture<Tuple<String[], IndexMetaData>> future = PlainActionFuture.newFuture();
        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(remoteClusterClient, remoteCluster,
            leaderIndex, future::onFailure,
            (historyUUID, leaderIndexMetaData) -> future.onResponse(new Tuple<>(historyUUID, leaderIndexMetaData)));
        Tuple<String[], IndexMetaData> tuple = future.actionGet();

        IndexMetaData leaderIndexMetaData = tuple.v2();

        IndexMetaData.Builder imdBuilder = IndexMetaData.builder(leaderIndexMetaData);
        // Adding the leader index uuid for each shard as custom metadata:
        Map<String, String> metadata = new HashMap<>();
        metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS, String.join(",", tuple.v1()));
        metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY, leaderIndexMetaData.getIndexUUID());
        metadata.put(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY, leaderIndexMetaData.getIndex().getName());
        metadata.put(Ccr.CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY, remoteCluster);
        imdBuilder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, metadata);

        imdBuilder.settings(leaderIndexMetaData.getSettings());

        // Copy mappings from leader IMD to follow IMD
        for (ObjectObjectCursor<String, MappingMetaData> cursor : leaderIndexMetaData.getMappings()) {
            imdBuilder.putMapping(cursor.value);
        }

        imdBuilder.setRoutingNumShards(leaderIndexMetaData.getRoutingNumShards());

        return imdBuilder.build();
    }

    @Override
    public RepositoryData getRepositoryData() {
        ClusterStateResponse response = client.getRemoteClusterClient("leader_cluster").admin().cluster().prepareState().clear().setMetaData(true).get();

        Map<String, SnapshotId> copiedSnapshotIds = this.snapshotMetaData.keySet().stream()
            .collect(Collectors.toMap(SnapshotId::getName, (sd) -> sd));
        Map<String, SnapshotState> snapshotStates = new HashMap<>(copiedSnapshotIds.size());
        Map<IndexId, Set<SnapshotId>> indexSnapshots = new HashMap<>(copiedSnapshotIds.size());
        for (Map.Entry<String, SnapshotId> snapshotId : copiedSnapshotIds.entrySet()) {
            snapshotStates.put(snapshotId.getKey(), SnapshotState.SUCCESS);
            MetaData metaData = client.admin().cluster().prepareState().clear().setMetaData(true).get().getState().getMetaData();
            ImmutableOpenMap<String, IndexMetaData> indices = metaData.indices();
            for (String indexName : metaData.getConcreteAllIndices()) {
                Index index = indices.get(indexName).getIndex();
                indexSnapshots.put(new IndexId(index.getName(), index.getUUID()), Collections.singleton(snapshotId.getValue()));
            }
        }

        return new RepositoryData(1, copiedSnapshotIds, snapshotStates, indexSnapshots, Collections.emptyList());
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData metaData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SnapshotInfo finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure, int totalShards,
                                         List<SnapshotShardFailure> shardFailures, long repositoryStateId, boolean includeGlobalState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return 0;
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return 0;
    }

    @Override
    public String startVerification() {
        return null;
    }

    @Override
    public void endVerification(String verificationToken) {

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
        throw new UnsupportedOperationException("Cannot snapshot shard for RemoteClusterRepository");
    }

    @Override
    public void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId,
                             RecoveryState recoveryState) {
        int i = 0;

    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
        throw new UnsupportedOperationException();
    }
}
