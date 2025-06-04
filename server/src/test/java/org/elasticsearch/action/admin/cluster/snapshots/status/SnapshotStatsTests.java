/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

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

    public void testXContentSerializationWhenProcessedFileCountEqualsIncrementalFileCount() throws IOException {
        final var instance = createTestInstance();
        final var incrementalSameAsProcessed = new SnapshotStats(
            instance.getStartTime(),
            instance.getTime(),
            instance.getIncrementalFileCount(),
            instance.getTotalFileCount(),
            instance.getIncrementalFileCount(), // processedFileCount
            instance.getIncrementalSize(),
            instance.getTotalSize(),
            instance.getIncrementalSize() // processedSize
        );
        // toXContent() omits the "processed" sub-object in this case, make sure the processed values are set as expected in fromXContent().
        testFromXContent(() -> incrementalSameAsProcessed);
    }

    public void testXContentSerializationForEmptyStats() throws IOException {
        testFromXContent(SnapshotStats::new);
    }

    @Override
    protected SnapshotStats doParseInstance(XContentParser parser) throws IOException {
        return SnapshotStats.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
