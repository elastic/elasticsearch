/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnapshotIndexCompletenessTests extends ESTestCase {

    private static final String REPO = "test-repo";
    private static final String SNAP = "test-snap";
    private static final String INDEX = "my-index";
    private static final String OTHER_INDEX = "other-index";

    private static Snapshot snapshot() {
        return new Snapshot(REPO, new SnapshotId(SNAP, randomAlphaOfLength(8)));
    }

    private static SnapshotInfo successSnapshot(List<String> indices) {
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        for (String index : indices) {
            details.put(index, new SnapshotInfo.IndexSnapshotDetails(randomIntBetween(1, 5), ByteSizeValue.ofBytes(1024), 1));
        }
        return new SnapshotInfo(
            snapshot(),
            indices,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            0L,
            indices.size(),
            Collections.emptyList(),
            null,
            null,
            0L,
            details
        );
    }

    private static SnapshotInfo partialSnapshot(List<String> indices, List<SnapshotShardFailure> failures) {
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        for (String index : indices) {
            long failedForIndex = failures.stream().filter(f -> index.equals(f.index())).count();
            int successfulShards = randomIntBetween(1, 5) - (int) Math.min(failedForIndex, 1);
            if (successfulShards > 0) {
                details.put(index, new SnapshotInfo.IndexSnapshotDetails(successfulShards, ByteSizeValue.ofBytes(512), 1));
            } else {
                details.put(index, SnapshotInfo.IndexSnapshotDetails.SKIPPED);
            }
        }
        return new SnapshotInfo(
            snapshot(),
            indices,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            0L,
            indices.size() * 2,
            failures,
            null,
            null,
            0L,
            details
        );
    }

    private static SnapshotShardFailure shardFailure(String indexName) {
        return new SnapshotShardFailure(null, new ShardId(indexName, randomAlphaOfLength(8), 0), "simulated failure");
    }

    public void testIndexNotInSnapshotIsNotComplete() {
        SnapshotInfo snap = successSnapshot(List.of(OTHER_INDEX));
        assertFalse(SnapshotIndexCompleteness.isComplete(snap, INDEX));
    }

    public void testSuccessSnapshotIndexIsComplete() {
        SnapshotInfo snap = successSnapshot(List.of(INDEX, OTHER_INDEX));
        assertTrue(SnapshotIndexCompleteness.isComplete(snap, INDEX));
        assertTrue(SnapshotIndexCompleteness.isComplete(snap, OTHER_INDEX));
    }

    public void testPartialSnapshotIndexWithNoFailuresIsComplete() {
        SnapshotInfo snap = partialSnapshot(List.of(INDEX, OTHER_INDEX), List.of(shardFailure(OTHER_INDEX)));
        assertTrue(SnapshotIndexCompleteness.isComplete(snap, INDEX));
    }

    public void testPartialSnapshotIndexWithFailureIsNotComplete() {
        SnapshotInfo snap = partialSnapshot(List.of(INDEX, OTHER_INDEX), List.of(shardFailure(INDEX)));
        assertFalse(SnapshotIndexCompleteness.isComplete(snap, INDEX));
    }

    public void testPartialSnapshotIndexWithMultipleShardFailuresIsNotComplete() {
        List<SnapshotShardFailure> failures = List.of(
            new SnapshotShardFailure(null, new ShardId(INDEX, randomAlphaOfLength(8), 0), "node left"),
            new SnapshotShardFailure(null, new ShardId(INDEX, randomAlphaOfLength(8), 1), "node left")
        );
        SnapshotInfo snap = partialSnapshot(List.of(INDEX), failures);
        assertFalse(SnapshotIndexCompleteness.isComplete(snap, INDEX));
    }

    public void testSkippedIndexIsNotComplete() {
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = Map.of(INDEX, SnapshotInfo.IndexSnapshotDetails.SKIPPED);
        SnapshotInfo snap = new SnapshotInfo(
            snapshot(),
            List.of(INDEX),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            0L,
            1,
            Collections.emptyList(),
            null,
            null,
            0L,
            details
        );
        assertFalse(SnapshotIndexCompleteness.isComplete(snap, INDEX));
    }

    public void testIndexAbsentFromDetailsIsCompleteOnSuccess() {
        // Legacy snapshots may omit indexSnapshotDetails; for SUCCESS state they are still complete.
        SnapshotInfo snap = new SnapshotInfo(
            snapshot(),
            List.of(INDEX),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            0L,
            1,
            Collections.emptyList(),
            null,
            null,
            0L,
            Collections.emptyMap()
        );
        assertTrue(SnapshotIndexCompleteness.isComplete(snap, INDEX));
    }
}
