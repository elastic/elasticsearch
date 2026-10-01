/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;

import java.util.List;

/**
 * Notified whenever a shard snapshot completes, with the shard's updated snapshot index.
 * <p>
 * The point of this is cost. What a shard's snapshots occupy is fully determined by
 * {@link BlobStoreIndexShardSnapshots}, which is assembled during every shard snapshot, written to the repository, and then discarded.
 * Recovering it afterwards means reading one blob per shard — fine occasionally, but not on a schedule, and not for a repository with a
 * hundred thousand shards. Observing it as it goes past costs nothing.
 * <p>
 * Implementations run on the snapshot path. They must be cheap, must not block, and must not throw: the shard snapshot has already
 * succeeded by the time this is called, and an observer must not be able to change that outcome. Anything substantial belongs on another
 * thread.
 */
public interface ShardSnapshotFilesObserver {

    /**
     * The default for deployments that do not track snapshot sizes, which is most of them.
     */
    ShardSnapshotFilesObserver NOOP = (projectRepo, indexId, shardId, shardGeneration, shardSnapshots) -> {};

    /**
     * @param projectRepo     the project and repository the snapshot was written to. Observers that care about only one repository must
     *                        filter on this — a cluster commonly has several, and conflating them would attribute one repository's bytes
     *                        to another
     * @param indexId         the repository's identity for the index, which is stable across the index being deleted and restored under
     *                        the same name, and so is the right key to record against
     * @param shardId         the shard's number within the index. Deliberately not a {@link org.elasticsearch.index.shard.ShardId}: that
     *                        carries the cluster's index UUID, which is not what the repository keys on and is not available on every path
     *                        this is called from
     * @param shardGeneration the generation written for this shard, identifying the blob now holding {@code shardSnapshots}
     * @param shardSnapshots  every snapshot of this shard that the repository currently retains, with the deduplicated set of files they
     *                        reference between them
     */
    void onShardSnapshotFilesUpdated(
        ProjectRepo projectRepo,
        IndexId indexId,
        int shardId,
        ShardGeneration shardGeneration,
        BlobStoreIndexShardSnapshots shardSnapshots
    );

    /**
     * Notified when a snapshot deletion has rewritten a shard's snapshots, with the repository-wide context that only the deleting node
     * has.
     * <p>
     * Kept separate from {@link #onShardSnapshotFilesUpdated} because the two happen in different places and carry different knowledge.
     * A shard snapshot runs on the node holding the shard, which knows nothing of the repository as a whole; a deletion runs where
     * {@link org.elasticsearch.repositories.RepositoryData} is available, so it can say which snapshots survive and in what order. An
     * observer that measures a shard against the oldest surviving snapshot can only do so here.
     * <p>
     * Same constraints as the other callback: cheap, non-blocking, and must not throw.
     *
     * @param retainedSnapshotNamesOldestFirst the names of every snapshot surviving this deletion, ordered by when they were taken. The
     *                                         ordering is repository-wide and identical for every shard in one deletion
     * @param indexInOldestRetainedSnapshot    whether the oldest surviving snapshot contains this shard's index at all. Distinguishes an
     *                                         index that did not yet exist from one whose shard merely was not captured — indistinguishable
     *                                         from the shard's own records, and opposite in meaning
     */
    default void onShardSnapshotFilesRebased(
        ProjectRepo projectRepo,
        IndexId indexId,
        int shardId,
        ShardGeneration shardGeneration,
        BlobStoreIndexShardSnapshots shardSnapshots,
        List<String> retainedSnapshotNamesOldestFirst,
        boolean indexInOldestRetainedSnapshot
    ) {}

    /**
     * Notified when the last snapshot referencing a shard has been deleted, so the shard occupies nothing in the repository any more.
     * <p>
     * Distinct from a shard whose snapshots are merely empty, and distinct from a shard absent from the newest snapshot: those are still
     * held by the repository and still cost something. This fires only once nothing references the shard at all, at which point an observer
     * tracking sizes should forget it rather than carry a figure for storage that no longer exists.
     * <p>
     * Same constraints as the other callbacks: cheap, non-blocking, and must not throw.
     */
    default void onShardSnapshotFilesRemoved(ProjectRepo projectRepo, IndexId indexId, int shardId) {}
}
