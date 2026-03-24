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
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexReshardService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;

public class SnapshotShardContextHelper {
    public static final Logger logger = LogManager.getLogger(SnapshotShardContextHelper.class);

    private SnapshotShardContextHelper() {}

    /**
     * Acquire an index commit for the shard snapshot, validating that the shard is a started primary and no resharding is in progress.
     * A {@code null} {@code snapshotStatus} means the snapshot is running on a remote node, abort handling and status updates skipped
     * on this node as they are handled on the remote node.
     */
    public static SnapshotIndexCommit acquireSnapshotIndexCommit(
        ClusterService clusterService,
        IndexShard indexShard,
        Snapshot snapshot,
        boolean supportsRelocationDuringSnapshot,
        @Nullable IndexShardSnapshotStatus snapshotStatus
    ) {
        final var shardId = indexShard.shardId();
        if (indexShard.routingEntry().primary() == false) {
            throw new IndexShardSnapshotFailedException(shardId, "snapshot should be performed only on primary");
        }
        // TODO: usage for supportsRelocationDuringSnapshot will be added in a future PR, see also ES-14099
        assert supportsRelocationDuringSnapshot == false;
        if (indexShard.routingEntry().relocating()) {
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

    public static void closeSnapshotIndexCommit(SnapshotIndexCommit snapshotIndexCommit, ShardId shardId, Snapshot snapshot) {
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
