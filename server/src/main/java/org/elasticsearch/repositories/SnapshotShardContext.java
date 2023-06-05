/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.snapshots.SnapshotId;

/**
 * Context holding the state for creating a shard snapshot via {@link Repository#snapshotShard(SnapshotShardContext)}.
 * Wraps a {@link org.elasticsearch.index.engine.Engine.IndexCommitRef} that is released once this instances is completed by invoking
 * either its {@link #onResponse(ShardSnapshotResult)} or {@link #onFailure(Exception)} callback.
 */
public final class SnapshotShardContext extends DelegatingActionListener<ShardSnapshotResult, ShardSnapshotResult> {

    private final Store store;
    private final MapperService mapperService;
    private final SnapshotId snapshotId;
    private final IndexId indexId;
    private final SnapshotIndexCommit commitRef;
    @Nullable
    private final String shardStateIdentifier;
    private final IndexShardSnapshotStatus snapshotStatus;
    private final Version repositoryMetaVersion;
    private final long snapshotStartTime;

    /**
     * @param store                 store to be snapshotted
     * @param mapperService         the shards mapper service
     * @param snapshotId            snapshot id
     * @param indexId               id for the index being snapshotted
     * @param commitRef             commit point reference
     * @param shardStateIdentifier  a unique identifier of the state of the shard that is stored with the shard's snapshot and used
     *                              to detect if the shard has changed between snapshots. If {@code null} is passed as the identifier
     *                              snapshotting will be done by inspecting the physical files referenced by {@code snapshotIndexCommit}
     * @param snapshotStatus        snapshot status
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param snapshotStartTime     start time of the snapshot found in
     *                              {@link org.elasticsearch.cluster.SnapshotsInProgress.Entry#startTime()}
     * @param listener              listener invoked on completion
     */
    public SnapshotShardContext(
        Store store,
        MapperService mapperService,
        SnapshotId snapshotId,
        IndexId indexId,
        SnapshotIndexCommit commitRef,
        @Nullable String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        Version repositoryMetaVersion,
        final long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) {
        super(new ActionListener<>() {
            @Override
            public void onResponse(ShardSnapshotResult shardSnapshotResult) {
                commitRef.onCompletion(listener.map(ignored -> shardSnapshotResult));
            }

            @Override
            public void onFailure(Exception e) {
                commitRef.onCompletion(listener.map(ignored -> {
                    throw e;
                }));
            }
        });
        this.store = store;
        this.mapperService = mapperService;
        this.snapshotId = snapshotId;
        this.indexId = indexId;
        this.commitRef = commitRef;
        this.shardStateIdentifier = shardStateIdentifier;
        this.snapshotStatus = snapshotStatus;
        this.repositoryMetaVersion = repositoryMetaVersion;
        this.snapshotStartTime = snapshotStartTime;
    }

    public Store store() {
        return store;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public SnapshotId snapshotId() {
        return snapshotId;
    }

    public IndexId indexId() {
        return indexId;
    }

    public IndexCommit indexCommit() {
        return commitRef.indexCommit();
    }

    @Nullable
    public String stateIdentifier() {
        return shardStateIdentifier;
    }

    public IndexShardSnapshotStatus status() {
        return snapshotStatus;
    }

    public Version getRepositoryMetaVersion() {
        return repositoryMetaVersion;
    }

    public long snapshotStartTime() {
        return snapshotStartTime;
    }

    @Override
    public void onResponse(ShardSnapshotResult result) {
        delegate.onResponse(result);
    }

    public Releasable withCommitRef() {
        snapshotStatus.ensureNotAborted(); // check this first to avoid acquiring a ref when aborted even if refs are available
        if (commitRef.tryIncRef()) {
            return Releasables.releaseOnce(commitRef::decRef);
        } else {
            snapshotStatus.ensureNotAborted();
            assert false : "commit ref closed early in state " + snapshotStatus;
            throw new IndexShardSnapshotFailedException(store.shardId(), "Store got closed concurrently");
        }
    }
}
