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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;

/**
 * Context for finalizing a snapshot.
 */
public final class FinalizeSnapshotContext extends ActionListener.Delegating<
    Tuple<RepositoryData, SnapshotInfo>,
    Tuple<RepositoryData, SnapshotInfo>> {

    private final ShardGenerations updatedShardGenerations;

    private final long repositoryStateId;

    private final Metadata clusterMetadata;

    private final SnapshotInfo snapshotInfo;

    private final Version repositoryMetaVersion;

    /**
     * @param updatedShardGenerations updated shard generations
     * @param repositoryStateId       the unique id identifying the state of the repository when the snapshot began
     * @param clusterMetadata         cluster metadata
     * @param snapshotInfo            SnapshotInfo instance to write for this snapshot
     * @param repositoryMetaVersion   version of the updated repository metadata to write
     * @param listener                listener to be invoked with the new {@link RepositoryData} and {@link SnapshotInfo} after completing
     *                                the snapshot
     */
    public FinalizeSnapshotContext(
        ShardGenerations updatedShardGenerations,
        long repositoryStateId,
        Metadata clusterMetadata,
        SnapshotInfo snapshotInfo,
        Version repositoryMetaVersion,
        ActionListener<Tuple<RepositoryData, SnapshotInfo>> listener
    ) {
        super(listener);
        this.updatedShardGenerations = updatedShardGenerations;
        this.repositoryStateId = repositoryStateId;
        this.clusterMetadata = clusterMetadata;
        this.snapshotInfo = snapshotInfo;
        this.repositoryMetaVersion = repositoryMetaVersion;
    }

    public long repositoryStateId() {
        return repositoryStateId;
    }

    public ShardGenerations updatedShardGenerations() {
        return updatedShardGenerations;
    }

    public SnapshotInfo snapshotInfo() {
        return snapshotInfo;
    }

    public Version repositoryMetaVersion() {
        return repositoryMetaVersion;
    }

    public Metadata clusterMetadata() {
        return clusterMetadata;
    }

    public ClusterState updatedClusterState(ClusterState state) {
        return SnapshotsService.stateWithoutSuccessfulSnapshot(state, snapshotInfo.snapshot());
    }

    @Override
    public void onResponse(Tuple<RepositoryData, SnapshotInfo> repositoryData) {
        delegate.onResponse(repositoryData);
    }
}
