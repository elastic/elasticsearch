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

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RemoteClusterRepository extends AbstractLifecycleComponent implements Repository {

    public final static SnapshotId SNAPSHOT_ID = new SnapshotId("_latest_", "_latest_");
    public final static String TYPE = "_remote_cluster_";

    private final RepositoryMetaData metadata;
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
        assert snapshotId.equals(SNAPSHOT_ID) : "RemoteClusterRepository only supports the SNAPSHOT_ID SnapshotId";
        ClusterStateResponse response = client.admin().cluster().prepareState().clear().setMetaData(true).get();
        return new SnapshotInfo(snapshotId, Arrays.asList(response.getState().metaData().getConcreteAllIndices()), SnapshotState.SUCCESS);
    }

    @Override
    public MetaData getSnapshotGlobalMetaData(SnapshotId snapshotId) {
        assert snapshotId.equals(SNAPSHOT_ID) : "RemoteClusterRepository only supports the SNAPSHOT_ID SnapshotId";
        ClusterStateResponse response = client.admin().cluster().prepareState().clear().setMetaData(true).get();
        return response.getState().metaData();
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
        assert snapshotId.equals(SNAPSHOT_ID) : "RemoteClusterRepository only supports the SNAPSHOT_ID SnapshotId";
        ClusterStateResponse response = client.admin().cluster().prepareState().clear().setMetaData(true).get();
        return response.getState().metaData().index(index.getName());
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
    public SnapshotInfo finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure, int totalShards, List<SnapshotShardFailure> shardFailures, long repositoryStateId, boolean includeGlobalState) {
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
    public void snapshotShard(IndexShard shard, Store store, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus) {
        throw new UnsupportedOperationException("Cannot snapshot shard for RemoteClusterRepository");
    }

    @Override
    public void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState) {

    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
        throw new UnsupportedOperationException();
    }
}
