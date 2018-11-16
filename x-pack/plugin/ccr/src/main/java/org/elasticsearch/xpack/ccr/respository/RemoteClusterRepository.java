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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
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
import org.elasticsearch.xpack.ccr.CcrSettings;

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
    public final static SnapshotId SNAPSHOT_ID = new SnapshotId("_latest_", SNAPSHOT_UUID);
    public final static String TYPE = "_remote_cluster_";

    private final CcrLicenseChecker ccrLicenseChecker = null;
    private final RepositoryMetaData metadata;
    private final Set<SnapshotId> snapshotIds = ConcurrentCollections.newConcurrentSet();
    private final Client client;

    public RemoteClusterRepository(RepositoryMetaData metadata, Client client, Settings settings) {
        super(settings);
        this.metadata = metadata;
        this.client = client;
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
        client.getRemoteClusterClient(remoteCluster);
        String leaderIndex = index.getName();
        PlainActionFuture<Tuple<String[], IndexMetaData>> future = PlainActionFuture.newFuture();
        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
            client,
            remoteCluster,
            leaderIndex,
            future::onFailure,
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

        // Copy all settings, but overwrite a few settings.
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(leaderIndexMetaData.getSettings());
        // Overwriting UUID here, because otherwise we can't follow indices in the same cluster
        settingsBuilder.put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
        // TODO: Figure out what to do with private setting SETTING_INDEX_PROVIDED_NAME
        settingsBuilder.put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, "");
        settingsBuilder.put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true);
        imdBuilder.settings(settingsBuilder);

        // Copy mappings from leader IMD to follow IMD
        for (ObjectObjectCursor<String, MappingMetaData> cursor : leaderIndexMetaData.getMappings()) {
            imdBuilder.putMapping(cursor.value);
        }

        imdBuilder.setRoutingNumShards(leaderIndexMetaData.getRoutingNumShards());

        return imdBuilder.build();
    }

    @Override
    public RepositoryData getRepositoryData() {
        MetaData metaData = client.admin().cluster().prepareState().clear().setMetaData(true).get().getState().getMetaData();
        return new RepositoryData(1,
            Collections.singletonMap(SNAPSHOT_ID.getName(), SNAPSHOT_ID),
            Collections.singletonMap(SNAPSHOT_ID.getName(), SnapshotState.SUCCESS),
            Arrays.stream(metaData.getConcreteAllIndices()).collect(Collectors.toMap(i -> {
                    Index index = metaData.indices().get(i).getIndex();
                    return new IndexId(index.getName(), index.getUUID());
                }, i -> Collections.singleton(SNAPSHOT_ID))),
            Collections.emptyList());
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
