/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.RepositoriesStats;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * A basic test suite designed to test whether the {@link BlobStoreSnapshotMetrics} class correctly increments and
 * decrements the corresponding metrics.
 * <p>
 * Note that there are two flavours of {@link BlobStoreSnapshotMetrics} depending on how the {@link ProjectId} is set.
 * When the {@link ProjectId} is set, then the {@link SnapshotMetrics} instance is also updated with the metric.
 * When the {@link ProjectId} is <i>not</i> set, then {@link BlobStoreSnapshotMetrics} uses {@link SnapshotMetrics#NOOP}.
 * Therefore, for completeness, all tests in this suite are run twice: once where the {@link ProjectId} is set, and once when it isn't.
 */
public class BlobStoreSnapshotMetricsTests extends ESTestCase {

    /**
     * Verifies that calling {@link BlobStoreSnapshotMetrics#incrementSnapshotRateLimitingTimeInNanos(long)}
     * correctly accumulates write throttling time.
     * <p>
     * This test increments the snapshot rate-limiting counter multiple times and asserts that:
     * <ul>
     *     <li>The total write throttled time is the sum of all increments.</li>
     *     <li>No other snapshot-related counters are affected.</li>
     * </ul>
     * The test is executed for both configurations of {@link BlobStoreSnapshotMetrics}:
     * with a {@link ProjectId} set and with a {@code null} {@link ProjectId}.
     */
    public void testIncrementSnapshotRateLimitingTimeInNanos() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            long firstSnapshotRateLimitingTimeInNanos = randomLongBetween(1_000L, 10_000L);
            metrics.incrementSnapshotRateLimitingTimeInNanos(firstSnapshotRateLimitingTimeInNanos);
            long secondSnapshotRateLimitingTimeInNanos = randomLongBetween(1_000L, 10_000L);
            metrics.incrementSnapshotRateLimitingTimeInNanos(secondSnapshotRateLimitingTimeInNanos);

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.shardSnapshotsStarted(), equalTo(0L));
            assertThat(stats.shardSnapshotsCompleted(), equalTo(0L));
            assertThat(stats.shardSnapshotsInProgress(), equalTo(0L));
            assertThat(stats.totalReadThrottledNanos(), equalTo(0L));
            assertThat(
                stats.totalWriteThrottledNanos(),
                equalTo(firstSnapshotRateLimitingTimeInNanos + secondSnapshotRateLimitingTimeInNanos)
            );
            assertThat(stats.numberOfBlobsUploaded(), equalTo(0L));
            assertThat(stats.numberOfBytesUploaded(), equalTo(0L));
            assertThat(stats.totalUploadTimeInMillis(), equalTo(0L));
            assertThat(stats.totalUploadReadTimeInMillis(), equalTo(0L));
        }
    }

    /**
     * Verifies that calling {@link BlobStoreSnapshotMetrics#incrementRestoreRateLimitingTimeInNanos(long)}
     * correctly accumulates read throttling time during restore operations.
     * <p>
     * This test ensures that:
     * <ul>
     *     <li>The total read throttled time reflects the sum of all increments.</li>
     *     <li>No other snapshot-related counters are affected.</li>
     * </ul>
     * The test is executed for both configurations of {@link BlobStoreSnapshotMetrics}:
     * with a {@link ProjectId} set and with a {@code null} {@link ProjectId}.
     */
    public void testIncrementRestoreRateLimitingTimeInNanos() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            long firstRestoreRateLimitingTimeInNanos = randomLongBetween(1_000L, 10_000L);
            metrics.incrementRestoreRateLimitingTimeInNanos(firstRestoreRateLimitingTimeInNanos);
            long secondRestoreRateLimitingTimeInNanos = randomLongBetween(1_000L, 10_000L);
            metrics.incrementRestoreRateLimitingTimeInNanos(secondRestoreRateLimitingTimeInNanos);

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.shardSnapshotsStarted(), equalTo(0L));
            assertThat(stats.shardSnapshotsCompleted(), equalTo(0L));
            assertThat(stats.shardSnapshotsInProgress(), equalTo(0L));
            assertThat(
                stats.totalReadThrottledNanos(),
                equalTo(firstRestoreRateLimitingTimeInNanos + secondRestoreRateLimitingTimeInNanos)
            );
            assertThat(stats.totalWriteThrottledNanos(), equalTo(0L));
            assertThat(stats.numberOfBlobsUploaded(), equalTo(0L));
            assertThat(stats.numberOfBytesUploaded(), equalTo(0L));
            assertThat(stats.totalUploadTimeInMillis(), equalTo(0L));
            assertThat(stats.totalUploadReadTimeInMillis(), equalTo(0L));
        }
    }

    /**
     * Verifies that part upload metrics are accumulated correctly when uploading snapshot data.
     * <p>
     * This test exercises {@link BlobStoreSnapshotMetrics#incrementCountersForPartUpload(long, long)}
     * and asserts that:
     * <ul>
     *     <li>The total number of uploaded bytes is incremented by the part sizes.</li>
     *     <li>The total upload time in milliseconds reflects the sum of the provided write times.</li>
     *     <li>No other snapshot-related counters are affected.</li>
     *  </ul>
     * The test is executed for both configurations of {@link BlobStoreSnapshotMetrics}:
     * with a {@link ProjectId} set and with a {@code null} {@link ProjectId}.
     */
    public void testIncrementCountersForPartUploadUsesNanos() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            long firstPartSizeInBytes = randomLongBetween(1024L, 8 * 1024L);
            long firstPartWriteTimeInMillis = randomLongBetween(1_000L, 10_000L);
            metrics.incrementCountersForPartUpload(firstPartSizeInBytes, firstPartWriteTimeInMillis);

            long secondPartSizeInBytes = randomLongBetween(1024L, 8 * 1024L);
            long secondPartWriteTimeInMillis = randomLongBetween(1_000L, 10_000L);
            metrics.incrementCountersForPartUpload(secondPartSizeInBytes, secondPartWriteTimeInMillis);

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.shardSnapshotsStarted(), equalTo(0L));
            assertThat(stats.shardSnapshotsCompleted(), equalTo(0L));
            assertThat(stats.shardSnapshotsInProgress(), equalTo(0L));
            assertThat(stats.totalReadThrottledNanos(), equalTo(0L));
            assertThat(stats.totalWriteThrottledNanos(), equalTo(0L));
            assertThat(stats.numberOfBlobsUploaded(), equalTo(0L));
            assertThat(stats.numberOfBytesUploaded(), equalTo(firstPartSizeInBytes + secondPartSizeInBytes));
            assertThat(stats.totalUploadTimeInMillis(), equalTo(firstPartWriteTimeInMillis + secondPartWriteTimeInMillis));
            assertThat(stats.totalUploadReadTimeInMillis(), equalTo(0L));
        }
    }

    /**
     * Verifies that the blob upload counter is incremented correctly.
     * <p>
     * This test repeatedly calls {@link BlobStoreSnapshotMetrics#incrementNumberOfBlobsUploaded()}
     * and asserts that:
     * <ul>
     *     <li>The number of uploaded blobs matches the number of increments.</li>
     *     <li>No other snapshot-related counters are affected.</li>
     * </ul>
     * The test is executed for both configurations of {@link BlobStoreSnapshotMetrics}:
     * with a {@link ProjectId} set and with a {@code null} {@link ProjectId}.
     */
    public void testIncrementNumberOfBlobsUploaded() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            int numberOfBlobsUploaded = randomIntBetween(5, 15);
            for (int i = 0; i < numberOfBlobsUploaded; i++) {
                metrics.incrementNumberOfBlobsUploaded();
            }

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.shardSnapshotsStarted(), equalTo(0L));
            assertThat(stats.shardSnapshotsCompleted(), equalTo(0L));
            assertThat(stats.shardSnapshotsInProgress(), equalTo(0L));
            assertThat(stats.totalReadThrottledNanos(), equalTo(0L));
            assertThat(stats.totalWriteThrottledNanos(), equalTo(0L));
            assertThat(stats.numberOfBlobsUploaded(), equalTo(Integer.toUnsignedLong(numberOfBlobsUploaded)));
            assertThat(stats.numberOfBytesUploaded(), equalTo(0L));
            assertThat(stats.totalUploadTimeInMillis(), equalTo(0L));
            assertThat(stats.totalUploadReadTimeInMillis(), equalTo(0L));
        }
    }

    /**
     * Verifies correct tracking of shard snapshot lifecycle counters.
     * <p>
     * This test simulates shard snapshot start and completion events and asserts that:
     * <ul>
     *     <li>The started counter is incremented for each shard snapshot start.</li>
     *     <li>The completed counter is incremented when snapshots complete.</li>
     *     <li>The in-progress counter reflects the difference between started and completed shards.</li>
     *     <li>No other snapshot-related counters are affected.</li>
     * </ul>
     * The test is executed for both configurations of {@link BlobStoreSnapshotMetrics}:
     * with a {@link ProjectId} set and with a {@code null} {@link ProjectId}.
     */
    public void testShardSnapshotLifecycleCounters() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            int numberOfShardsStarted = randomIntBetween(5, 15);
            for (int i = 0; i < numberOfShardsStarted; i++) {
                metrics.shardSnapshotStarted();
            }
            assertThat(metrics.getShardSnapshotsInProgress().value(), equalTo(Integer.toUnsignedLong(numberOfShardsStarted)));

            IndexShardSnapshotStatus status = Mockito.mock(IndexShardSnapshotStatus.class);
            Mockito.when(status.getStage()).thenReturn(randomFrom(IndexShardSnapshotStatus.Stage.values()));
            Mockito.when(status.getTotalTimeMillis()).thenReturn(randomLong());
            int numberOfShardsCompleted = randomIntBetween(1, numberOfShardsStarted);
            for (int i = 0; i < numberOfShardsCompleted; i++) {
                metrics.shardSnapshotCompleted(status);
            }

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.shardSnapshotsStarted(), equalTo(Integer.toUnsignedLong(numberOfShardsStarted)));
            assertThat(stats.shardSnapshotsCompleted(), equalTo(Integer.toUnsignedLong(numberOfShardsCompleted)));
            assertThat(stats.shardSnapshotsInProgress(), equalTo(Integer.toUnsignedLong(numberOfShardsStarted - numberOfShardsCompleted)));
            assertThat(stats.totalReadThrottledNanos(), equalTo(0L));
            assertThat(stats.totalWriteThrottledNanos(), equalTo(0L));
            assertThat(stats.numberOfBlobsUploaded(), equalTo(0L));
            assertThat(stats.numberOfBytesUploaded(), equalTo(0L));
            assertThat(stats.totalUploadTimeInMillis(), equalTo(0L));
            assertThat(stats.totalUploadReadTimeInMillis(), equalTo(0L));
        }
    }

    /**
     * Verifies that upload read time is accumulated correctly.
     * <p>
     * This test increments the upload read time multiple times via
     * {@link BlobStoreSnapshotMetrics#incrementUploadReadTime(long)} and asserts that:
     * <ul>
     *     <li>The total upload read time reflects the sum of all increments.</li>
     *     <li>No other snapshot-related counters are affected.</li>
     * </ul>
     * The test is executed for both configurations of {@link BlobStoreSnapshotMetrics}:
     * with a {@link ProjectId} set and with a {@code null} {@link ProjectId}.
     */
    public void testIncrementUploadReadTimeStoredAsNanosCounter() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            long firstUploadReadTime = randomLongBetween(1_000L, 10_000L);
            metrics.incrementUploadReadTime(firstUploadReadTime);
            long secondUploadReadTime = randomLongBetween(1_000L, 10_000L);
            metrics.incrementUploadReadTime(secondUploadReadTime);

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.shardSnapshotsStarted(), equalTo(0L));
            assertThat(stats.shardSnapshotsCompleted(), equalTo(0L));
            assertThat(stats.shardSnapshotsInProgress(), equalTo(0L));
            assertThat(stats.totalReadThrottledNanos(), equalTo(0L));
            assertThat(stats.totalWriteThrottledNanos(), equalTo(0L));
            assertThat(stats.numberOfBlobsUploaded(), equalTo(0L));
            assertThat(stats.numberOfBytesUploaded(), equalTo(0L));
            assertThat(stats.totalUploadTimeInMillis(), equalTo(0L));
            assertThat(stats.totalUploadReadTimeInMillis(), equalTo(firstUploadReadTime + secondUploadReadTime));
        }
    }

    private List<BlobStoreSnapshotMetrics> getMetrics() {
        return List.of(getBlobStoreSnapshotMetrics(ProjectId.DEFAULT), getBlobStoreSnapshotMetrics(null));
    }

    private BlobStoreSnapshotMetrics getBlobStoreSnapshotMetrics(ProjectId projectId) {
        RepositoryMetadata repoMetadata = new RepositoryMetadata("repo", "type", Settings.EMPTY);
        return new BlobStoreSnapshotMetrics(projectId, repoMetadata, new SnapshotMetrics(new RecordingMeterRegistry()));
    }
}
