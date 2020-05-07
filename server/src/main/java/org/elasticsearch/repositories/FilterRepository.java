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
package org.elasticsearch.repositories;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class FilterRepository implements Repository {

    private final Repository in;

    public FilterRepository(Repository in) {
        this.in = in;
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return in.getMetadata();
    }

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        return in.getSnapshotInfo(snapshotId);
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        return in.getSnapshotGlobalMetadata(snapshotId);
    }

    @Override
    public IndexMetadata getSnapshotIndexMetadata(SnapshotId snapshotId, IndexId index) throws IOException {
        return in.getSnapshotIndexMetadata(snapshotId, index);
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        in.getRepositoryData(listener);
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId, ShardGenerations shardGenerations, long startTime, String failure,
                                 int totalShards, List<SnapshotShardFailure> shardFailures, long repositoryStateId,
                                 boolean includeGlobalState, Metadata metadata, Map<String, Object> userMetadata,
                                 Version repositoryMetaVersion, Function<ClusterState, ClusterState> stateTransformer,
                                 ActionListener<Tuple<RepositoryData, SnapshotInfo>> listener) {
        in.finalizeSnapshot(snapshotId, shardGenerations, startTime, failure, totalShards, shardFailures, repositoryStateId,
            includeGlobalState, metadata, userMetadata, repositoryMetaVersion, stateTransformer, listener);
    }

    @Override
    public void deleteSnapshots(Collection<SnapshotId> snapshotIds, long repositoryStateId, Version repositoryMetaVersion,
                                ActionListener<Void> listener) {
        in.deleteSnapshots(snapshotIds, repositoryStateId, repositoryMetaVersion, listener);
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return in.getSnapshotThrottleTimeInNanos();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return in.getRestoreThrottleTimeInNanos();
    }

    @Override
    public String startVerification() {
        return in.startVerification();
    }

    @Override
    public void endVerification(String verificationToken) {
        in.endVerification(verificationToken);
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {
        in.verify(verificationToken, localNode);
    }

    @Override
    public boolean isReadOnly() {
        return in.isReadOnly();
    }

    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, String shardStateIdentifier, IndexShardSnapshotStatus snapshotStatus,
                              Version repositoryMetaVersion, Map<String, Object> userMetadata, ActionListener<String> listener) {
        in.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, shardStateIdentifier, snapshotStatus,
            repositoryMetaVersion, userMetadata, listener);
    }
    @Override
    public void restoreShard(Store store, SnapshotId snapshotId, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState,
                             ActionListener<Void> listener) {
        in.restoreShard(store, snapshotId, indexId, snapshotShardId, recoveryState, listener);
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        return in.getShardSnapshotStatus(snapshotId, indexId, shardId);
    }

    @Override
    public void updateState(ClusterState state) {
        in.updateState(state);
    }

    @Override
    public void executeConsistentStateUpdate(Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask, String source,
                                             Consumer<Exception> onFailure) {
        in.executeConsistentStateUpdate(createUpdateTask, source, onFailure);
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return in.lifecycleState();
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        in.addLifecycleListener(listener);
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        in.removeLifecycleListener(listener);
    }

    @Override
    public void start() {
        in.start();
    }

    @Override
    public void stop() {
        in.stop();
    }

    @Override
    public void close() {
        in.close();
    }
}
