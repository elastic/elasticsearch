/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

public class RepositoriesStatsTests extends ESTestCase {

    public void testToXContent() throws IOException {
        int numberOfMapEntries = randomIntBetween(3,5);
        Map<String, RepositoriesStats.SnapshotStats> repositoryStatsMap = new HashMap<>();
        for (int i = 0; i < numberOfMapEntries; i++) {
            repositoryStatsMap.put(
                randomAlphaOfLength(10),
                randomBoolean() ?
                    RepositoriesStats.SnapshotStats.ZERO :
                    // TODO - Replace the zero with a call to SnapshotStatsUtils
                    RepositoriesStats.SnapshotStats.ZERO
            );
        }
        final RepositoriesStats repositoriesStats = new RepositoriesStats(repositoryStatsMap);

        // Build XContent
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        repositoriesStats.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();

        // Convert back to map so we can assert on it
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();

        assertEquals(1, map.size());
        @SuppressWarnings("unchecked")
        Map<String, RepositoriesStats.SnapshotStats> returnedRepositories = (Map<String, RepositoriesStats.SnapshotStats>) map.get("repositories");
        assertEquals(numberOfMapEntries, returnedRepositories.size());

        var originalKeySet = repositoryStatsMap.keySet();
        var newKeySet = returnedRepositories.keySet();
        assertEquals(originalKeySet, newKeySet);

        // SnapshotStats are checked individually in their own test suite
        for (Object value : returnedRepositories.values()) {
            assertTrue(value instanceof Map);
        }
    }

    public void testImmutabilityOfReturnedMap() {
        final RepositoriesStats stats = new RepositoriesStats(
            Map.of(
                "repo",
                randomBoolean() ?
                    RepositoriesStats.SnapshotStats.ZERO :
                    // TODO - Replace the zero with a call to SnapshotStatsUtils
                    RepositoriesStats.SnapshotStats.ZERO
            )
        );
        assertThrows(
            UnsupportedOperationException.class,
            () -> stats.getRepositorySnapshotStats().put(
                "other",
                RepositoriesStats.SnapshotStats.ZERO
            )
        );
    }

    // TODO - Add this to snapshot stats bwc tests
    // TODO - If not already, extend the BWC tests added in prev PR to assert all other values are constant
    // since we currently just use a random long

    /**
     * Verifies wire serialization when both the writing and reading nodes support
     * {@link RepositoriesStats.SnapshotStats#EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO}.
     *
     * <p>
     * This test ensures that:
     * <ul>
     *   <li>All extended snapshot statistics fields are written to the stream</li>
     *   <li>All extended fields are correctly read back</li>
     *   <li>No data loss occurs when communicating between nodes on the same
     *       (or newer) transport version</li>
     * </ul>
     *
     * <p>
     * This protects against accidental removal, reordering, or omission of
     * fields guarded by the transport version check in
     * {@link RepositoriesStats.SnapshotStats#writeTo(StreamOutput)}.
     */
    public void testExtendedSnapshotStatsSerializationWithNewVersion() throws IOException {
        TransportVersion version = RepositoriesStats.SnapshotStats.EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO;

        long shardSnapshotsStarted = randomNonNegativeLong();
        long shardSnapshotsCompleted = randomNonNegativeLong();
        long shardSnapshotsInProgress = randomNonNegativeLong();
        long totalReadThrottledNanos = randomNonNegativeLong();
        long totalWriteThrottledNanos = randomNonNegativeLong();
        long numberOfBlobsUploaded = randomNonNegativeLong();
        long numberOfBytesUploaded = randomNonNegativeLong();
        long totalUploadTimeInMillis = randomNonNegativeLong();
        long totalUploadReadTimeInMillis = randomNonNegativeLong();
        RepositoriesStats.SnapshotStats snapshotStats = new RepositoriesStats.SnapshotStats(
            shardSnapshotsStarted,
            shardSnapshotsCompleted,
            shardSnapshotsInProgress,
            totalReadThrottledNanos,
            totalWriteThrottledNanos,
            numberOfBlobsUploaded,
            numberOfBytesUploaded,
            totalUploadTimeInMillis,
            totalUploadReadTimeInMillis
        );

        RepositoriesStats original = new RepositoriesStats(Map.of("repo", snapshotStats));

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(version);
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(version);

        RepositoriesStats deserialized = new RepositoriesStats(in);
        RepositoriesStats.SnapshotStats result =
            deserialized.getRepositorySnapshotStats().get("repo");

        assertThat(result.shardSnapshotsStarted(), equalTo(shardSnapshotsStarted));
        assertThat(result.shardSnapshotsCompleted(), equalTo(shardSnapshotsCompleted));
        assertThat(result.shardSnapshotsInProgress(), equalTo(shardSnapshotsInProgress));
        assertThat(result.totalReadThrottledNanos(), equalTo(totalReadThrottledNanos));
        assertThat(result.totalWriteThrottledNanos(), equalTo(totalWriteThrottledNanos));
        assertThat(result.numberOfBlobsUploaded(), equalTo(numberOfBlobsUploaded));
        assertThat(result.numberOfBytesUploaded(), equalTo(numberOfBytesUploaded));
        assertThat(result.totalUploadTimeInMillis(), equalTo(totalUploadTimeInMillis));
        assertThat(result.totalUploadReadTimeInMillis(), equalTo(totalUploadReadTimeInMillis));
    }

    // TODO - Add this to snapshot stats bwc tests

    /**
     * Verifies backward compatibility when a node running a newer transport version
     * sends {@link RepositoriesStats} to a node that does NOT support
     * {@link RepositoriesStats.SnapshotStats#EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO}.
     *
     * <p>
     * This test ensures that:
     * <ul>
     *   <li>Only legacy fields are read by the older node</li>
     *   <li>Extended snapshot statistics are safely skipped</li>
     *   <li>No stream corruption or read misalignment occurs</li>
     *   <li>Extended fields are defaulted to zero values</li>
     * </ul>
     *
     * <p>
     * This is critical for rolling upgrades, where new nodes may send extended
     * statistics to older nodes that are still part of the cluster.
     */
    public void testExtendedSnapshotStatsDroppedWhenReadingOldVersion() throws IOException {
        TransportVersion newVersion = RepositoriesStats.SnapshotStats.EXTENDED_SNAPSHOT_STATS_IN_NODE_INFO;
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(newVersion);

        long totalReadThrottledNanos = randomNonNegativeLong();
        long totalWriteThrottledNanos = randomNonNegativeLong();
        RepositoriesStats.SnapshotStats snapshotStats = new RepositoriesStats.SnapshotStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            totalReadThrottledNanos,
            totalWriteThrottledNanos,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );

        RepositoriesStats original = new RepositoriesStats(Map.of("repo", snapshotStats));

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(newVersion);
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);

        RepositoriesStats deserialized = new RepositoriesStats(in);
        RepositoriesStats.SnapshotStats result =
            deserialized.getRepositorySnapshotStats().get("repo");

        // Only legacy fields survive
        assertThat(result.totalReadThrottledNanos(), equalTo(totalReadThrottledNanos));
        assertThat(result.totalWriteThrottledNanos(), equalTo(totalWriteThrottledNanos));

        // Extended fields must be zeroed
        assertThat(result.shardSnapshotsStarted(), equalTo(0L));
        assertThat(result.shardSnapshotsCompleted(), equalTo(0L));
        assertThat(result.shardSnapshotsInProgress(), equalTo(0L));
        assertThat(result.numberOfBlobsUploaded(), equalTo(0L));
        assertThat(result.numberOfBytesUploaded(), equalTo(0L));
        assertThat(result.totalUploadTimeInMillis(), equalTo(0L));
        assertThat(result.totalUploadReadTimeInMillis(), equalTo(0L));
    }
}
