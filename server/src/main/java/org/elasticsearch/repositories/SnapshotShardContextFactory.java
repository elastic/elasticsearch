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
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexReshardService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * Factory interface to create {@link SnapshotShardContext} instances.
 */
public interface SnapshotShardContextFactory {

    Logger logger = LogManager.getLogger(SnapshotShardContextFactory.class);

    /**
     * Asynchronously creates a {@link SnapshotShardContext} for the given shard and snapshot. The passed-in listener is
     * notified once the shard snapshot completes, either successfully or with a failure.
     * @param shardId Shard ID of the shard to snapshot
     * @param snapshot The overall snapshot information
     * @param indexId The index ID of the index in the snapshot
     * @param snapshotStatus Status of the shard snapshot
     * @param repositoryMetaVersion The repository metadata version this snapshot uses
     * @param snapshotStartTime Start time of the overall snapshot
     * @param listener Listener to be invoked once the shard snapshot completes
     * @return A subscribable listener that provides the created {@link SnapshotShardContext} or an exception if creation failed.
     * @throws IOException Exception that may throw before even the returning subscribable listener is created. When this happens,
     * the passed-in listener will NOT be notified. Caller is responsible to handle this situation.
     */
    SubscribableListener<SnapshotShardContext> asyncCreate(
        ShardId shardId,
        Snapshot snapshot,
        IndexId indexId,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) throws IOException;

    /**
     * Indicates whether the factory supports relocating a shard while its snapshot is in progress. When {@code true},
     * lifecycle of the local shard is not tied to its shard snapshot. For example, when the shard closes, it does
     * not automatically abort the snapshot {@link org.elasticsearch.snapshots.SnapshotShardsService#beforeIndexShardClosed}.
     * Note this value indicates whether the feature is supported, but whether relocation will actually happen still depends
     * on other factors {@link org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider}
     */
    default boolean supportsRelocationDuringSnapshot() {
        return false;
    }

    /**
     * Acquire an index commit for the shard snapshot and ensure it's valid with respect to resharding.
     */
    static SnapshotIndexCommit acquireSnapshotIndexCommit(
        ClusterService clusterService,
        IndexShard indexShard,
        Snapshot snapshot,
        boolean supportsRelocationDuringSnapshot,
        @Nullable IndexShardSnapshotStatus snapshotStatus // null when the shard snapshot runs on a remote node
    ) {
        final var shardId = indexShard.shardId();
        if (indexShard.routingEntry().primary() == false) {
            throw new IndexShardSnapshotFailedException(shardId, "snapshot should be performed only on primary");
        }
        if (supportsRelocationDuringSnapshot == false && indexShard.routingEntry().relocating()) {
            // do not snapshot when in the process of relocation of primaries so we won't get conflicts
            throw new IndexShardSnapshotFailedException(shardId, "cannot snapshot while relocating");
        }

        final IndexShardState indexShardState = indexShard.state();
        if (indexShardState == IndexShardState.CREATED || indexShardState == IndexShardState.RECOVERING) {
            // shard has just been created, or still recovering
            throw new IndexShardSnapshotFailedException(shardId, "shard didn't fully recover yet");
        }

        if (snapshotStatus != null) {
            snapshotStatus.updateStatusDescription("acquiring commit reference from IndexShard: triggers a shard flush");
        }
        final var snapshotIndexCommit = new SnapshotIndexCommit(indexShard.acquireIndexCommitForSnapshot());
        try {

            // The check below is needed to handle shard snapshots during resharding.
            // Resharding changes the number of shards in the index and moves data between shards.
            // These processes may cause shard snapshots to be inconsistent with each other (e.g. caught in between data movements)
            // or to be out of sync with index metadata (e.g. a newly added shard is not present in the snapshot).
            // We want to detect if a resharding operation has happened after this snapshot was started
            // and if so we'll fail the shard snapshot to avoid such inconsistency.
            // We perform this check here on the data node and not on the master node
            // to correctly propagate this failure to SnapshotsService using existing listener
            // in case resharding starts in the middle of the snapshot.
            // Marking shard as failed directly in the cluster state would bypass parts of SnapshotsService logic.

            // We obtain a new `SnapshotsInProgress.Entry` here in order to not capture the original in the Runnable.
            // The information that we are interested in (the shards map keys) doesn't change so this is fine.
            SnapshotsInProgress.Entry snapshotEntry = SnapshotsInProgress.get(clusterService.state()).snapshot(snapshot);
            // The snapshot is deleted, there is no reason to proceed.
            if (snapshotEntry == null) {
                throw new IndexShardSnapshotFailedException(shardId, "snapshot is deleted");
            }

            int maximumShardIdForIndexInTheSnapshot = calculateMaximumShardIdForIndexInTheSnapshot(shardId, snapshotEntry);
            if (IndexReshardService.isShardSnapshotImpactedByResharding(
                indexShard.indexSettings().getIndexMetadata(),
                maximumShardIdForIndexInTheSnapshot
            )) {
                throw new IndexShardSnapshotFailedException(shardId, "cannot snapshot a shard during resharding");
            }

            if (snapshotStatus != null) {
                snapshotStatus.updateStatusDescription("commit reference acquired, proceeding with snapshot");
                snapshotStatus.addAbortListener(makeAbortListener(indexShard.shardId(), snapshot, snapshotIndexCommit));
                snapshotStatus.ensureNotAborted();
            }
            return snapshotIndexCommit;
        } catch (Exception e) {
            closeSnapshotIndexCommit(snapshotIndexCommit, shardId, snapshot);
            throw e;
        }
    }

    static void closeSnapshotIndexCommit(SnapshotIndexCommit snapshotIndexCommit, ShardId shardId, Snapshot snapshot) {
        snapshotIndexCommit.closingBefore(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                // we're already failing exceptionally, and prefer to propagate the original exception instead of this one
                logger.warn(Strings.format("exception closing commit for [%s] in [%s]", shardId, snapshot), e);
            }
        }).onResponse(null);
    }

    private static int calculateMaximumShardIdForIndexInTheSnapshot(ShardId shardIdStartingASnapshot, SnapshotsInProgress.Entry entry) {
        int maximum = shardIdStartingASnapshot.id();
        int i = maximum + 1;

        while (entry.shards().containsKey(new ShardId(shardIdStartingASnapshot.getIndex(), i))) {
            maximum = i;
            i += 1;
        }

        return maximum;
    }

    private static ActionListener<IndexShardSnapshotStatus.AbortStatus> makeAbortListener(
        ShardId shardId,
        Snapshot snapshot,
        SnapshotIndexCommit snapshotIndexCommit
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(IndexShardSnapshotStatus.AbortStatus abortStatus) {
                if (abortStatus == IndexShardSnapshotStatus.AbortStatus.ABORTED) {
                    assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC, ThreadPool.Names.SNAPSHOT);
                    snapshotIndexCommit.onAbort();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> Strings.format("unexpected failure in %s", description()), e);
                assert false : e;
            }

            @Override
            public String toString() {
                return description();
            }

            private String description() {
                return Strings.format("abort listener for [%s] in [%s]", shardId, snapshot);
            }
        };
    }
}
