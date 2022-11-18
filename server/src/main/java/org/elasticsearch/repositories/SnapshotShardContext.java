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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.snapshots.SnapshotId;

import java.util.Map;

/**
 * Context holding the state for creating a shard snapshot via {@link Repository#snapshotShard(SnapshotShardContext)}.
 * Wraps a {@link org.elasticsearch.index.engine.Engine.IndexCommitRef} that is released once this instances is completed by invoking
 * either its {@link #onResponse(ShardSnapshotResult)} or {@link #onFailure(Exception)} callback.
 */
public final class SnapshotShardContext extends ActionListener.Delegating<ShardSnapshotResult, ShardSnapshotResult> {

    private final Store store;
    private final MapperService mapperService;
    private final SnapshotId snapshotId;
    private final IndexId indexId;
    private final Engine.IndexCommitRef commitRef;
    @Nullable
    private final String shardStateIdentifier;
    private final IndexShardSnapshotStatus snapshotStatus;
    private final Version repositoryMetaVersion;
    private final Map<String, Object> userMetadata;
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
     * @param userMetadata          user metadata of the snapshot found in
     *                              {@link org.elasticsearch.cluster.SnapshotsInProgress.Entry#userMetadata()}
     * @param snapshotStartTime     start time of the snapshot found in
     *                              {@link org.elasticsearch.cluster.SnapshotsInProgress.Entry#startTime()}
     * @param listener              listener invoked on completion
     */
    public SnapshotShardContext(
        Store store,
        MapperService mapperService,
        SnapshotId snapshotId,
        IndexId indexId,
        Engine.IndexCommitRef commitRef,
        @Nullable String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        Version repositoryMetaVersion,
        Map<String, Object> userMetadata,
        final long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) {
        super(ActionListener.runBefore(listener, commitRef::close));
        this.store = store;
        this.mapperService = mapperService;
        this.snapshotId = snapshotId;
        this.indexId = indexId;
        this.commitRef = commitRef;
        this.shardStateIdentifier = shardStateIdentifier;
        this.snapshotStatus = snapshotStatus;
        this.repositoryMetaVersion = repositoryMetaVersion;
        this.userMetadata = userMetadata;
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
        return commitRef.getIndexCommit();
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

    public Map<String, Object> userMetadata() {
        return userMetadata;
    }

    public long snapshotStartTime() {
        return snapshotStartTime;
    }

    @Override
    public void onResponse(ShardSnapshotResult result) {
        delegate.onResponse(result);
    }
}
