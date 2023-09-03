/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotDeleteListener;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.Collection;

/**
 * This class represents a repository that could not be initialized due to unknown type.
 * This could happen when a user creates a snapshot repository using a type from a plugin and then removes the plugin.
 */
public class UnknownTypeRepository extends AbstractLifecycleComponent implements Repository {

    private final RepositoryMetadata repositoryMetadata;

    public UnknownTypeRepository(RepositoryMetadata repositoryMetadata) {
        this.repositoryMetadata = repositoryMetadata;
    }

    private RepositoryException createUnknownTypeException() {
        return new RepositoryException(
            repositoryMetadata.name(),
            "repository type [" + repositoryMetadata.type() + "] is unknown; ensure that all required plugins are installed on this node"
        );
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return repositoryMetadata;
    }

    @Override
    public void getSnapshotInfo(GetSnapshotInfoContext context) {
        throw createUnknownTypeException();
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        throw createUnknownTypeException();
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) throws IOException {
        throw createUnknownTypeException();
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        listener.onFailure(createUnknownTypeException());
    }

    @Override
    public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
        finalizeSnapshotContext.onFailure(createUnknownTypeException());
    }

    @Override
    public void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        IndexVersion repositoryMetaVersion,
        SnapshotDeleteListener listener
    ) {
        listener.onFailure(createUnknownTypeException());
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        throw createUnknownTypeException();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        throw createUnknownTypeException();
    }

    @Override
    public String startVerification() {
        throw createUnknownTypeException();
    }

    @Override
    public void endVerification(String verificationToken) {
        throw createUnknownTypeException();
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {
        throw createUnknownTypeException();
    }

    @Override
    public boolean isReadOnly() {
        // this repository is assumed writable to bypass read-only check and fail with exception produced by this class
        return false;
    }

    @Override
    public void snapshotShard(SnapshotShardContext snapshotShardContext) {
        snapshotShardContext.onFailure(createUnknownTypeException());
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
        listener.onFailure(createUnknownTypeException());
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        throw createUnknownTypeException();
    }

    @Override
    public void updateState(ClusterState state) {

    }

    @Override
    public void cloneShardSnapshot(
        SnapshotId source,
        SnapshotId target,
        RepositoryShardId shardId,
        ShardGeneration shardGeneration,
        ActionListener<ShardSnapshotResult> listener
    ) {
        listener.onFailure(createUnknownTypeException());
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
