/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * Represents a repository that exists in the cluster state but could not be instantiated on a node, typically due to invalid configuration.
 */
public class InvalidRepository extends AbstractLifecycleComponent implements Repository {

    private final RepositoryMetadata repositoryMetadata;
    private final RepositoryException creationException;

    @FixForMultiProject(description = "constructor needs to take a ProjectId parameter")
    @Deprecated(forRemoval = true)
    public InvalidRepository(RepositoryMetadata repositoryMetadata, RepositoryException creationException) {
        this.repositoryMetadata = repositoryMetadata;
        this.creationException = creationException;
    }

    private RepositoryException createCreationException() {
        return new RepositoryException(
            repositoryMetadata.name(),
            "repository type [" + repositoryMetadata.type() + "] failed to create on current node",
            creationException
        );
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return repositoryMetadata;
    }

    @Override
    public void getSnapshotInfo(
        Collection<SnapshotId> snapshotIds,
        boolean abortOnFailure,
        BooleanSupplier isCancelled,
        CheckedConsumer<SnapshotInfo, Exception> consumer,
        ActionListener<Void> listener
    ) {
        listener.onFailure(createCreationException());
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        throw createCreationException();
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) throws IOException {
        throw createCreationException();
    }

    @Override
    public void getRepositoryData(Executor responseExecutor, ActionListener<RepositoryData> listener) {
        listener.onFailure(createCreationException());
    }

    @Override
    public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
        finalizeSnapshotContext.onFailure(createCreationException());
    }

    @Override
    public void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryDataGeneration,
        IndexVersion minimumNodeVersion,
        ActionListener<RepositoryData> repositoryDataUpdateListener,
        Runnable onCompletion
    ) {
        repositoryDataUpdateListener.onFailure(createCreationException());
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        throw createCreationException();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        throw createCreationException();
    }

    @Override
    public String startVerification() {
        throw createCreationException();
    }

    @Override
    public void endVerification(String verificationToken) {
        throw createCreationException();
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {
        throw createCreationException();
    }

    @Override
    public boolean isReadOnly() {
        // this repository is assumed writable to bypass read-only check and fail with exception produced by this class
        return false;
    }

    @Override
    public void snapshotShard(SnapshotShardContext snapshotShardContext) {
        snapshotShardContext.onFailure(createCreationException());
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
        listener.onFailure(createCreationException());
    }

    @Override
    public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        throw createCreationException();
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
        listener.onFailure(createCreationException());
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
