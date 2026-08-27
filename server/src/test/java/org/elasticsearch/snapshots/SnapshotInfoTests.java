/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnapshotInfoTests extends ESTestCase {

    private static final String REPO = "test-repo";
    private static final String SNAP = "test-snap";
    private static final String INDEX = "my-index";
    private static final String OTHER_INDEX = "other-index";
    private static final String DS_NAME = "logs-app";
    private static final String BACKING_1 = ".ds-logs-app-000001";
    private static final String BACKING_2 = ".ds-logs-app-000002";
    private static final String FAILURE_1 = ".fs-logs-app-000001";

    private static Snapshot snapshot() {
        return new Snapshot(REPO, new SnapshotId(SNAP, randomAlphaOfLength(8)));
    }

    private static Index index(String name) {
        return new Index(name, randomAlphaOfLength(8));
    }

    private static SnapshotInfo.IndexSnapshotDetails successDetails() {
        return new SnapshotInfo.IndexSnapshotDetails(randomIntBetween(1, 3), ByteSizeValue.ofBytes(512), 1);
    }

    private static SnapshotShardFailure shardFailure(String indexName) {
        return new SnapshotShardFailure(null, new ShardId(indexName, randomAlphaOfLength(8), 0), "simulated failure");
    }

    private static SnapshotInfo successSnapshot(List<String> indices) {
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        for (String idx : indices) {
            details.put(idx, new SnapshotInfo.IndexSnapshotDetails(randomIntBetween(1, 5), ByteSizeValue.ofBytes(1024), 1));
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
        for (String idx : indices) {
            long failedForIndex = failures.stream().filter(f -> idx.equals(f.index())).count();
            int successfulShards = randomIntBetween(1, 5) - (int) Math.min(failedForIndex, 1);
            if (successfulShards > 0) {
                details.put(idx, new SnapshotInfo.IndexSnapshotDetails(successfulShards, ByteSizeValue.ofBytes(512), 1));
            } else {
                details.put(idx, SnapshotInfo.IndexSnapshotDetails.SKIPPED);
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

    private static SnapshotInfo snapshotWithDataStream(
        List<String> dataStreams,
        List<String> indices,
        Map<String, SnapshotInfo.IndexSnapshotDetails> indexDetails,
        List<SnapshotShardFailure> failures
    ) {
        return new SnapshotInfo(
            snapshot(),
            indices,
            dataStreams,
            Collections.emptyList(),
            null,
            0L,
            indices.size() * 2,
            failures,
            null,
            null,
            0L,
            indexDetails
        );
    }

    // -------------------------------------------------------------------------
    // isIndexComplete
    // -------------------------------------------------------------------------

    public void testIndexNotInSnapshotIsNotComplete() {
        SnapshotInfo snap = successSnapshot(List.of(OTHER_INDEX));
        assertFalse(snap.isIndexComplete(INDEX));
    }

    public void testSuccessSnapshotIndexIsComplete() {
        SnapshotInfo snap = successSnapshot(List.of(INDEX, OTHER_INDEX));
        assertTrue(snap.isIndexComplete(INDEX));
        assertTrue(snap.isIndexComplete(OTHER_INDEX));
    }

    public void testPartialSnapshotIndexWithNoFailuresIsComplete() {
        SnapshotInfo snap = partialSnapshot(List.of(INDEX, OTHER_INDEX), List.of(shardFailure(OTHER_INDEX)));
        assertTrue(snap.isIndexComplete(INDEX));
    }

    public void testPartialSnapshotIndexWithFailureIsNotComplete() {
        SnapshotInfo snap = partialSnapshot(List.of(INDEX, OTHER_INDEX), List.of(shardFailure(INDEX)));
        assertFalse(snap.isIndexComplete(INDEX));
    }

    public void testPartialSnapshotIndexWithMultipleShardFailuresIsNotComplete() {
        List<SnapshotShardFailure> failures = List.of(
            new SnapshotShardFailure(null, new ShardId(INDEX, randomAlphaOfLength(8), 0), "node left"),
            new SnapshotShardFailure(null, new ShardId(INDEX, randomAlphaOfLength(8), 1), "node left")
        );
        SnapshotInfo snap = partialSnapshot(List.of(INDEX), failures);
        assertFalse(snap.isIndexComplete(INDEX));
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
        assertFalse(snap.isIndexComplete(INDEX));
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
        assertTrue(snap.isIndexComplete(INDEX));
    }

    // -------------------------------------------------------------------------
    // isDataStreamComplete
    // -------------------------------------------------------------------------

    public void testDataStreamNotInSnapshotIsNotComplete() {
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put(BACKING_1, successDetails());
        DataStream ds = DataStreamTestHelper.newInstance(DS_NAME, List.of(index(BACKING_1)));

        SnapshotInfo snap = snapshotWithDataStream(List.of(), List.of(BACKING_1), details, List.of());

        assertFalse(snap.isDataStreamComplete(ds));
    }

    public void testSuccessSnapshotDataStreamWithOneBackingIndexIsComplete() {
        DataStream ds = DataStreamTestHelper.newInstance(DS_NAME, List.of(index(BACKING_1)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put(BACKING_1, successDetails());
        SnapshotInfo snap = snapshotWithDataStream(List.of(DS_NAME), List.of(BACKING_1), details, List.of());

        assertTrue(snap.isDataStreamComplete(ds));
    }

    public void testSuccessSnapshotDataStreamWithMultipleBackingIndicesIsComplete() {
        DataStream ds = DataStreamTestHelper.newInstance(DS_NAME, List.of(index(BACKING_1), index(BACKING_2)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put(BACKING_1, successDetails());
        details.put(BACKING_2, successDetails());
        SnapshotInfo snap = snapshotWithDataStream(List.of(DS_NAME), List.of(BACKING_1, BACKING_2), details, List.of());

        assertTrue(snap.isDataStreamComplete(ds));
    }

    public void testSuccessSnapshotDataStreamWithFailureStoreIsComplete() {
        // Failure-store indices are not checked — completeness depends only on backing indices.
        DataStream ds = DataStreamTestHelper.newInstance(DS_NAME, List.of(index(BACKING_1)), List.of(index(FAILURE_1)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put(BACKING_1, successDetails());
        details.put(FAILURE_1, SnapshotInfo.IndexSnapshotDetails.SKIPPED);
        SnapshotInfo snap = snapshotWithDataStream(List.of(DS_NAME), List.of(BACKING_1, FAILURE_1), details, List.of(shardFailure(FAILURE_1)));

        assertTrue(snap.isDataStreamComplete(ds));
    }

    public void testPartialSnapshotWithFailedBackingIndexIsNotComplete() {
        DataStream ds = DataStreamTestHelper.newInstance(DS_NAME, List.of(index(BACKING_1), index(BACKING_2)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put(BACKING_1, successDetails());
        details.put(BACKING_2, SnapshotInfo.IndexSnapshotDetails.SKIPPED);
        SnapshotInfo snap = snapshotWithDataStream(
            List.of(DS_NAME),
            List.of(BACKING_1, BACKING_2),
            details,
            List.of(shardFailure(BACKING_2))
        );

        assertFalse(snap.isDataStreamComplete(ds));
    }

    public void testPartialSnapshotWithAllBackingIndicesCompleteIsComplete() {
        DataStream ds = DataStreamTestHelper.newInstance(DS_NAME, List.of(index(BACKING_1), index(BACKING_2)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put(BACKING_1, successDetails());
        details.put(BACKING_2, successDetails());
        SnapshotInfo snap = snapshotWithDataStream(
            List.of(DS_NAME),
            List.of(BACKING_1, BACKING_2),
            details,
            List.of(shardFailure("other-index"))
        );

        assertTrue(snap.isDataStreamComplete(ds));
    }

    public void testBackingIndexMissingFromSnapshotIsNotComplete() {
        DataStream ds = DataStreamTestHelper.newInstance(DS_NAME, List.of(index(BACKING_1), index(BACKING_2)));
        Map<String, SnapshotInfo.IndexSnapshotDetails> details = new HashMap<>();
        details.put(BACKING_1, successDetails());
        SnapshotInfo snap = snapshotWithDataStream(List.of(DS_NAME), List.of(BACKING_1), details, List.of());

        assertFalse(snap.isDataStreamComplete(ds));
    }
}
