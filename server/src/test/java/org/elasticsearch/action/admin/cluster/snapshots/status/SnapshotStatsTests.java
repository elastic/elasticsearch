/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class SnapshotStatsTests extends AbstractXContentTestCase<SnapshotStats> {

    @Override
    protected SnapshotStats createTestInstance() {
        // Using less than half of Long.MAX_VALUE for random time values to avoid long overflow in tests that add the two time values
        long startTime = randomLongBetween(0, Long.MAX_VALUE / 2 - 1);
        long time = randomLongBetween(0, Long.MAX_VALUE / 2 - 1);
        int incrementalFileCount = randomIntBetween(0, Integer.MAX_VALUE);
        int totalFileCount = randomIntBetween(0, Integer.MAX_VALUE);
        int processedFileCount = randomIntBetween(0, Integer.MAX_VALUE);
        long incrementalSize = ((long) randomIntBetween(0, Integer.MAX_VALUE)) * 2;
        long totalSize = ((long) randomIntBetween(0, Integer.MAX_VALUE)) * 2;
        long processedSize = ((long) randomIntBetween(0, Integer.MAX_VALUE)) * 2;
        return new SnapshotStats(
            startTime,
            time,
            incrementalFileCount,
            totalFileCount,
            processedFileCount,
            incrementalSize,
            totalSize,
            processedSize
        );
    }

    @Override
    protected SnapshotStats doParseInstance(XContentParser parser) throws IOException {
        return SnapshotStats.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testMissingStats() throws IOException {
        final var populatedStats = createTestInstance();
        final var missingStats = SnapshotStats.forMissingStats();
        assertEquals(0L, missingStats.getStartTime());
        assertEquals(0L, missingStats.getTime());
        assertEquals(-1, missingStats.getTotalFileCount());
        assertEquals(-1, missingStats.getIncrementalFileCount());
        assertEquals(-1, missingStats.getProcessedFileCount());
        assertEquals(-1L, missingStats.getTotalSize());
        assertEquals(-1L, missingStats.getIncrementalSize());
        assertEquals(-1L, missingStats.getProcessedSize());

        // Verify round trip Transport serialization.
        for (var transportVersion : List.of(
            TransportVersions.MINIMUM_COMPATIBLE,
            TransportVersions.SNAPSHOT_INDEX_SHARD_STATUS_MISSING_STATS,
            TransportVersion.current()
        )) {

            for (var stats : List.of(populatedStats, missingStats)) {
                final var bytesOut = new ByteArrayOutputStream();

                try (var streamOut = new OutputStreamStreamOutput(bytesOut)) {
                    streamOut.setTransportVersion(transportVersion);

                    if (transportVersion.onOrAfter(TransportVersions.SNAPSHOT_INDEX_SHARD_STATUS_MISSING_STATS) || stats != missingStats) {
                        stats.writeTo(streamOut);
                    } else {
                        assertThrows(IllegalStateException.class, () -> stats.writeTo(streamOut));
                        continue;
                    }
                }

                try (var streamIn = new ByteArrayStreamInput(bytesOut.toByteArray())) {
                    streamIn.setTransportVersion(transportVersion);
                    final var statsRead = new SnapshotStats(streamIn);
                    assertEquals(stats, statsRead);
                }
            }
        }

        // Verify round trip XContent serialization.
        testFromXContent(SnapshotStats::forMissingStats);
    }
}
