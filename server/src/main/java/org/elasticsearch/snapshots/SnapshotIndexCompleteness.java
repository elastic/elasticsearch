/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

/**
 * Evaluates whether a snapshot completely captures an index — i.e., every shard of the index was
 * successfully stored. Used by the source-discovery API to filter out indices that can only be
 * partially restored from a given recovery point.
 * <p>
 * An index is <em>complete</em> when all of the following hold:
 * <ul>
 *   <li>The index appears in {@link SnapshotInfo#indices()}.</li>
 *   <li>The index was not skipped: its {@link SnapshotInfo.IndexSnapshotDetails#getShardCount()} is
 *       greater than zero (or the entry is absent, indicating a legacy snapshot where shard details
 *       were not recorded).</li>
 *   <li>None of the snapshot's {@link SnapshotInfo#shardFailures()} reference the index.</li>
 * </ul>
 */
public final class SnapshotIndexCompleteness {

    private SnapshotIndexCompleteness() {}

    /**
     * Returns {@code true} if {@code indexName} is completely captured in {@code snapshotInfo}.
     *
     * @param snapshotInfo the snapshot to examine; must be in state {@link SnapshotState#SUCCESS}
     *                     or {@link SnapshotState#PARTIAL} (callers should already exclude
     *                     {@link SnapshotState#FAILED} and {@link SnapshotState#IN_PROGRESS} snapshots)
     * @param indexName    the index to evaluate
     */
    public static boolean isComplete(SnapshotInfo snapshotInfo, String indexName) {
        if (snapshotInfo.indices().contains(indexName) == false) {
            return false;
        }
        SnapshotInfo.IndexSnapshotDetails details = snapshotInfo.indexSnapshotDetails().get(indexName);
        if (details != null && details.getShardCount() == 0) {
            // Index was explicitly skipped during snapshotting — no shards were captured.
            return false;
        }
        if (snapshotInfo.state() == SnapshotState.SUCCESS) {
            // SUCCESS means no shard failures at all; every included index is complete.
            return true;
        }
        // PARTIAL snapshot: the index is complete only if none of its shards failed.
        return snapshotInfo.shardFailures().stream().noneMatch(f -> indexName.equals(f.index()));
    }
}
