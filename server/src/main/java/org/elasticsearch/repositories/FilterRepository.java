/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

public class FilterRepository implements Repository {

    private final Repository in;

    public FilterRepository(Repository in) {
        this.in = in;
    }

    public Repository getDelegate() {
        return in;
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return in.getMetadata();
    }

    @Override
    public void getSnapshotInfo(GetSnapshotInfoContext context) {
        in.getSnapshotInfo(context);
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        return in.getSnapshotGlobalMetadata(snapshotId);
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) throws IOException {
        return in.getSnapshotIndexMetaData(repositoryData, snapshotId, index);
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        in.getRepositoryData(listener);
    }

    @Override
    public void finalizeSnapshot(
        ShardGenerations shardGenerations,
        long repositoryStateId,
        Metadata clusterMetadata,
        SnapshotInfo snapshotInfo,
        Version repositoryMetaVersion,
        Function<ClusterState, ClusterState> stateTransformer,
        ActionListener<RepositoryData> listener
    ) {
        in.finalizeSnapshot(
            shardGenerations,
            repositoryStateId,
            clusterMetadata,
            snapshotInfo,
            repositoryMetaVersion,
            stateTransformer,
            listener
        );
    }

    @Override
    public void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        Version repositoryMetaVersion,
        ActionListener<RepositoryData> listener
    ) {
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
    public void snapshotShard(SnapshotShardContext context) {
        in.snapshotShard(context);
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
        in.restoreShard(store, snapshotId, indexId, snapshotShardId, recoveryState, listener);
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        return in.getShardSnapshotStatus(snapshotId, indexId, shardId);
    }

    @Override
    public boolean canUpdateInPlace(Settings updatedSettings, Set<String> ignoredSettings) {
        return in.canUpdateInPlace(updatedSettings, ignoredSettings);
    }

    @Override
    public void updateState(ClusterState state) {
        in.updateState(state);
    }

    @Override
    public void executeConsistentStateUpdate(
        Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask,
        String source,
        Consumer<Exception> onFailure
    ) {
        in.executeConsistentStateUpdate(createUpdateTask, source, onFailure);
    }

    @Override
    public void cloneShardSnapshot(
        SnapshotId source,
        SnapshotId target,
        RepositoryShardId shardId,
        String shardGeneration,
        ActionListener<ShardSnapshotResult> listener
    ) {
        in.cloneShardSnapshot(source, target, shardId, shardGeneration, listener);
    }

    @Override
    public void awaitIdle() {
        in.awaitIdle();
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
