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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This class represents a repository that still exists in the global cluster state but could not be loaded as its plugin was uninstalled.
 */
public class MissingPluginRepository extends AbstractLifecycleComponent implements Repository {

    private final RepositoryMetadata repositoryMetadata;

    public MissingPluginRepository(RepositoryMetadata repositoryMetadata) {
        this.repositoryMetadata = repositoryMetadata;
    }

    private RepositoryPluginException createMissingPluginException() {
        return new RepositoryPluginException(
            repositoryMetadata.name(),
            "the plugin is not installed for repository type [" + repositoryMetadata.type() + "]"
        );
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return repositoryMetadata;
    }

    @Override
    public void getSnapshotInfo(GetSnapshotInfoContext context) {
        throw createMissingPluginException();
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        throw createMissingPluginException();
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) throws IOException {
        throw createMissingPluginException();
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        listener.onFailure(createMissingPluginException());
    }

    @Override
    public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
        finalizeSnapshotContext.onFailure(createMissingPluginException());
    }

    @Override
    public void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        Version repositoryMetaVersion,
        ActionListener<RepositoryData> listener
    ) {
        listener.onFailure(createMissingPluginException());
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        throw createMissingPluginException();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        throw createMissingPluginException();
    }

    @Override
    public String startVerification() {
        throw createMissingPluginException();
    }

    @Override
    public void endVerification(String verificationToken) {
        throw createMissingPluginException();
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {
        throw createMissingPluginException();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void snapshotShard(SnapshotShardContext snapshotShardContext) {
        snapshotShardContext.onFailure(createMissingPluginException());
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
        listener.onFailure(createMissingPluginException());
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        throw createMissingPluginException();
    }

    @Override
    public void updateState(ClusterState state) {

    }

    @Override
    public void executeConsistentStateUpdate(
        Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask,
        String source,
        Consumer<Exception> onFailure
    ) {
        onFailure.accept(createMissingPluginException());
    }

    @Override
    public void cloneShardSnapshot(
        SnapshotId source,
        SnapshotId target,
        RepositoryShardId shardId,
        ShardGeneration shardGeneration,
        ActionListener<ShardSnapshotResult> listener
    ) {
        listener.onFailure(createMissingPluginException());
    }

    @Override
    public void awaitIdle() {

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
}
