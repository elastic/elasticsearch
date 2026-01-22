/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link RepositoriesStats.SnapshotStats#toXContent}
 */
public class SnapshotStatsXContentTests extends ESTestCase {

    /**
     * Tests all fields are present
     * @throws IOException
     */
    public void testToXContent() throws IOException {
        long shardSnapshotsStarted = randomNonNegativeLong();
        long shardSnapshotsCompleted = randomNonNegativeLong();
        long shardSnapshotsInProgress = randomNonNegativeLong();
        long totalReadThrottledNanos = randomNonNegativeLong();
        long totalWriteThrottledNanos = randomNonNegativeLong();
        long numberOfBlobsUploaded = randomNonNegativeLong();
        long numberOfBytesUploaded = randomNonNegativeLong();
        long totalUploadTimeInNanos = randomNonNegativeLong();
        long totalUploadReadTimeInNanos = randomNonNegativeLong();
        RepositoriesStats.SnapshotStats stats = new RepositoriesStats.SnapshotStats(
            shardSnapshotsStarted,
            shardSnapshotsCompleted,
            shardSnapshotsInProgress,
            totalReadThrottledNanos,
            totalWriteThrottledNanos,
            numberOfBlobsUploaded,
            numberOfBytesUploaded,
            totalUploadTimeInNanos,
            totalUploadReadTimeInNanos
        );

        // Build XContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // Convert back to map so we can assert on it
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();

        assertEquals(totalReadThrottledNanos, map.get("total_read_throttled_time_nanos"));
        assertEquals(totalWriteThrottledNanos, map.get("total_write_throttled_time_nanos"));
        assertEquals(shardSnapshotsStarted, map.get("shard_snapshots_started"));
        assertEquals(shardSnapshotsCompleted, map.get("shard_snapshots_completed"));
        assertEquals(shardSnapshotsInProgress, map.get("shard_snapshots_in_progress"));
        assertEquals(numberOfBlobsUploaded, map.get("uploaded_blobs"));
        assertEquals(numberOfBytesUploaded, map.get("uploaded_size_in_bytes"));

        // Assert the milliseconds fields correctly convert the time from nanoseconds to milliseconds
        assertEquals(TimeUnit.NANOSECONDS.toMillis(totalUploadTimeInNanos), map.get("total_upload_time_in_millis"));
        assertEquals(TimeUnit.NANOSECONDS.toMillis(totalUploadReadTimeInNanos), map.get("total_read_time_in_millis"));

        assertEquals(totalUploadTimeInNanos, map.get("total_upload_time_in_nanos"));
        assertEquals(totalUploadReadTimeInNanos, map.get("total_read_time_in_nanos"));
    }

    /**
     * Tests all fields are present with their human readable names
     * @throws IOException
     */
    public void testToXContentWithHumanReadableNames() throws IOException {
        long shardSnapshotsStarted = randomNonNegativeLong();
        long shardSnapshotsCompleted = randomNonNegativeLong();
        long shardSnapshotsInProgress = randomNonNegativeLong();
        long totalReadThrottledNanos = randomNonNegativeLong();
        long totalWriteThrottledNanos = randomNonNegativeLong();
        long numberOfBlobsUploaded = randomNonNegativeLong();
        long numberOfBytesUploaded = randomNonNegativeLong();
        long totalUploadTimeInNanos = randomNonNegativeLong();
        long totalUploadReadTimeInNanos = randomNonNegativeLong();
        RepositoriesStats.SnapshotStats stats = new RepositoriesStats.SnapshotStats(
            shardSnapshotsStarted,
            shardSnapshotsCompleted,
            shardSnapshotsInProgress,
            totalReadThrottledNanos,
            totalWriteThrottledNanos,
            numberOfBlobsUploaded,
            numberOfBytesUploaded,
            totalUploadTimeInNanos,
            totalUploadReadTimeInNanos
        );

        // Build XContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.humanReadable(true);
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);

        // Convert back to map so we can assert on it
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();

        assertEquals(totalReadThrottledNanos, map.get("total_read_throttled_time"));
        assertEquals(totalWriteThrottledNanos, map.get("total_write_throttled_time"));
        assertEquals(shardSnapshotsStarted, map.get("shard_snapshots_started"));
        assertEquals(shardSnapshotsCompleted, map.get("shard_snapshots_completed"));
        assertEquals(shardSnapshotsInProgress, map.get("shard_snapshots_in_progress"));
        assertEquals(numberOfBlobsUploaded, map.get("uploaded_blobs"));
        assertEquals(numberOfBytesUploaded, map.get("uploaded_size"));

        // Assert the milliseconds fields correctly convert the time from nanoseconds to milliseconds
        assertEquals(TimeUnit.NANOSECONDS.toMillis(totalUploadTimeInNanos), map.get("total_upload_time"));
        assertEquals(TimeUnit.NANOSECONDS.toMillis(totalUploadReadTimeInNanos), map.get("total_read_time"));

        assertEquals(totalUploadTimeInNanos, map.get("total_upload_time_nanos"));
        assertEquals(totalUploadReadTimeInNanos, map.get("total_read_time_nanos"));
    }
}
