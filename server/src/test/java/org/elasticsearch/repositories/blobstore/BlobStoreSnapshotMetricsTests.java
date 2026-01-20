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
    // TODO - Javadoc
    public void testIncrementSnapshotRateLimitingTimeInNanos() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            long firstSnapshotRateLimitingTimeInNanos = randomLongBetween(1_000L, 10_000L);
            metrics.incrementSnapshotRateLimitingTimeInNanos(firstSnapshotRateLimitingTimeInNanos);
            long secondSnapshotRateLimitingTimeInNanos = randomLongBetween(1_000L, 10_000L);
            metrics.incrementSnapshotRateLimitingTimeInNanos(secondSnapshotRateLimitingTimeInNanos);

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.totalWriteThrottledNanos(), equalTo(firstSnapshotRateLimitingTimeInNanos + secondSnapshotRateLimitingTimeInNanos));
        }
    }

    public void testIncrementRestoreRateLimitingTimeInNanos() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            long firstRestoreRateLimitingTimeInNanos = randomLongBetween(1_000L, 10_000L);
            metrics.incrementRestoreRateLimitingTimeInNanos(firstRestoreRateLimitingTimeInNanos);
            long secondRestoreRateLimitingTimeInNanos = randomLongBetween(1_000L, 10_000L);
            metrics.incrementRestoreRateLimitingTimeInNanos(secondRestoreRateLimitingTimeInNanos);

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.totalReadThrottledNanos(), equalTo(firstRestoreRateLimitingTimeInNanos + secondRestoreRateLimitingTimeInNanos));
        }
    }

    public void testIncrementCountersForPartUploadUsesNanos() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            long firstPartSizeInBytes = randomLongBetween(1024L, 8 * 1024L);
            long firstPartWriteTimeInMillis = randomLongBetween(1_000L, 10_000L);
            metrics.incrementCountersForPartUpload(firstPartSizeInBytes, firstPartWriteTimeInMillis);

            long secondPartSizeInBytes = randomLongBetween(1024L, 8 * 1024L);
            long secondPartWriteTimeInMillis = randomLongBetween(1_000L, 10_000L);
            metrics.incrementCountersForPartUpload(secondPartSizeInBytes, secondPartWriteTimeInMillis);

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.numberOfBytesUploaded(), equalTo(firstPartSizeInBytes + secondPartSizeInBytes));
            assertThat(stats.totalUploadTimeInMillis(), equalTo(firstPartWriteTimeInMillis + secondPartWriteTimeInMillis));
        }
    }

    public void testIncrementNumberOfBlobsUploaded() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            int numberOfBlobsUploaded = randomIntBetween(5, 15);
            for (int i = 0; i < numberOfBlobsUploaded; i++) {
                metrics.incrementNumberOfBlobsUploaded();
            }

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.numberOfBlobsUploaded(), equalTo(Integer.toUnsignedLong(numberOfBlobsUploaded)));
        }
    }

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
        }
    }

    public void testIncrementUploadReadTimeStoredAsNanosCounter() {
        for (BlobStoreSnapshotMetrics metrics : getMetrics()) {
            long firstUploadReadTime = randomLongBetween(1_000L, 10_000L);
            metrics.incrementUploadReadTime(firstUploadReadTime);
            long secondUploadReadTime = randomLongBetween(1_000L, 10_000L);
            metrics.incrementUploadReadTime(secondUploadReadTime);

            RepositoriesStats.SnapshotStats stats = metrics.getSnapshotStats();
            assertThat(stats.totalUploadReadTimeInMillis(), equalTo(firstUploadReadTime + secondUploadReadTime));
        }
    }

    // TODO - Tests that test the value used for the metrics: millis versus nanos

    private List<BlobStoreSnapshotMetrics> getMetrics() {
        return List.of(getBlobStoreSnapshotMetrics(), getBlobStoreSnapshotMetrics(null));
    }

    private BlobStoreSnapshotMetrics getBlobStoreSnapshotMetrics() {
        return getBlobStoreSnapshotMetrics(ProjectId.DEFAULT);
    }

    private BlobStoreSnapshotMetrics getBlobStoreSnapshotMetrics(ProjectId projectId) {
        RepositoryMetadata repoMetadata = new RepositoryMetadata(
            "repo",
            "type",
            Settings.EMPTY
        );
        return new BlobStoreSnapshotMetrics(
            projectId,
            repoMetadata,
            new SnapshotMetrics(new RecordingMeterRegistry())
        );
    }
}
