/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Context for finalizing a snapshot.
 */
public final class FinalizeSnapshotContext extends DelegatingActionListener<RepositoryData, RepositoryData> {

    private final UpdatedShardGenerations updatedShardGenerations;

    /**
     * Obsolete shard generations map computed from the cluster state update that this finalization executed in
     * {@link #updatedClusterState}.
     */
    private final SetOnce<Map<RepositoryShardId, Set<ShardGeneration>>> obsoleteGenerations = new SetOnce<>();

    private final long repositoryStateId;

    private final Metadata clusterMetadata;

    private final SnapshotInfo snapshotInfo;

    private final IndexVersion repositoryMetaVersion;

    private final Runnable onDone;

    /**
     * @param updatedShardGenerations updated shard generations for both live and deleted indices
     * @param repositoryStateId       the unique id identifying the state of the repository when the snapshot began
     * @param clusterMetadata         cluster metadata
     * @param snapshotInfo            SnapshotInfo instance to write for this snapshot
     * @param repositoryMetaVersion   version of the updated repository metadata to write
     * @param listener                listener to be invoked with the new {@link RepositoryData} after the snapshot has been successfully
     *                                added to the repository
     * @param onDone                  consumer of the new {@link SnapshotInfo} for the snapshot that is invoked after the {@code listener}
     *                                once all cleanup operations after snapshot completion have executed
     */
    public FinalizeSnapshotContext(
        UpdatedShardGenerations updatedShardGenerations,
        long repositoryStateId,
        Metadata clusterMetadata,
        SnapshotInfo snapshotInfo,
        IndexVersion repositoryMetaVersion,
        ActionListener<RepositoryData> listener,
        Runnable onDone
    ) {
        super(listener);
        this.updatedShardGenerations = updatedShardGenerations;
        this.repositoryStateId = repositoryStateId;
        this.clusterMetadata = clusterMetadata;
        this.snapshotInfo = snapshotInfo;
        this.repositoryMetaVersion = repositoryMetaVersion;
        this.onDone = onDone;
    }

    public long repositoryStateId() {
        return repositoryStateId;
    }

    public UpdatedShardGenerations updatedShardGenerations() {
        return updatedShardGenerations;
    }

    public SnapshotInfo snapshotInfo() {
        return snapshotInfo;
    }

    public IndexVersion repositoryMetaVersion() {
        return repositoryMetaVersion;
    }

    public Metadata clusterMetadata() {
        return clusterMetadata;
    }

    public Map<RepositoryShardId, Set<ShardGeneration>> obsoleteShardGenerations() {
        assert obsoleteGenerations.get() != null : "must only be called after #updatedClusterState";
        return obsoleteGenerations.get();
    }

    /**
     * Returns a new {@link ClusterState}, based on the given {@code state} with the create-snapshot entry removed.
     */
    public ClusterState updatedClusterState(ClusterState state) {
        final ClusterState updatedState = SnapshotsService.stateWithoutSnapshot(state, snapshotInfo.snapshot(), updatedShardGenerations);
        // Now that the updated cluster state may have changed in-progress shard snapshots' shard generations to the latest shard
        // generation, let's mark any now unreferenced shard generations as obsolete and ready to be deleted.

        final Collection<IndexId> deletedIndices = updatedShardGenerations.deletedIndices.indices();
        obsoleteGenerations.set(
            SnapshotsInProgress.get(updatedState)
                .obsoleteGenerations(snapshotInfo.repository(), SnapshotsInProgress.get(state))
                .entrySet()
                .stream()
                // We want to keep both old and new generations for deleted indices, so we filter them out here to avoid deletion.
                // We need the old generations because they are what get recorded in the RepositoryData.
                // We also need the new generations a future finalization may build upon them. It may ends up not being used at all
                // when current batch of in-progress snapshots are completed and no new index of the same name is created.
                // That is also OK. It means we have some redundant shard generations in the repository and they will be deleted
                // when a snapshot deletion runs.
                .filter(e -> deletedIndices.contains(e.getKey().index()) == false)
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
        );
        return updatedState;
    }

    public void onDone() {
        onDone.run();
    }

    @Override
    public void onResponse(RepositoryData repositoryData) {
        delegate.onResponse(repositoryData);
    }

    /**
     * A record used to track the new shard generations that have been written for each shard in a snapshot.
     * An index may be deleted after the shard generation is written but before the snapshot is finalized.
     * In this case, its shard generation is tracked in {@link #deletedIndices}. Otherwise, it is tracked in
     * {@link #liveIndices}.
     */
    public record UpdatedShardGenerations(ShardGenerations liveIndices, ShardGenerations deletedIndices) {
        public static final UpdatedShardGenerations EMPTY = new UpdatedShardGenerations(ShardGenerations.EMPTY, ShardGenerations.EMPTY);

        public UpdatedShardGenerations(ShardGenerations updated) {
            this(updated, ShardGenerations.EMPTY);
        }

        public boolean hasShardGen(RepositoryShardId repositoryShardId) {
            return liveIndices.hasShardGen(repositoryShardId) || deletedIndices.hasShardGen(repositoryShardId);
        }
    }
}
