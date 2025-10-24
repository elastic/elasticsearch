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
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.RepositoriesStats;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.telemetry.metric.LongWithAttributes;

import java.util.Map;

public class BlobStoreSnapshotMetrics {

    private final SnapshotMetrics snapshotMetrics;
    private final CounterMetric shardSnapshotsInProgress = new CounterMetric();
    private final CounterMetric snapshotRateLimitingTimeInNanos = new CounterMetric();
    private final CounterMetric restoreRateLimitingTimeInNanos = new CounterMetric();
    private final CounterMetric numberOfBlobsUploaded = new CounterMetric();
    private final CounterMetric numberOfBytesUploaded = new CounterMetric();
    private final CounterMetric uploadTimeInMillis = new CounterMetric();
    private final CounterMetric uploadReadTimeInNanos = new CounterMetric();
    private final CounterMetric numberOfShardSnapshotsStarted = new CounterMetric();
    private final CounterMetric numberOfShardSnapshotsCompleted = new CounterMetric();
    private final Map<String, Object> metricAttributes;

    public BlobStoreSnapshotMetrics(@Nullable ProjectId projectId, RepositoryMetadata repositoryMetadata, SnapshotMetrics snapshotMetrics) {
        if (projectId != null) {
            this.snapshotMetrics = snapshotMetrics;
            metricAttributes = SnapshotMetrics.createAttributesMap(projectId, repositoryMetadata);
        } else {
            // Project ID should only be null for the stateless main blobstore, which is not used for snapshots
            this.snapshotMetrics = SnapshotMetrics.NOOP;
            this.metricAttributes = Map.of();
        }
    }

    public void incrementSnapshotRateLimitingTimeInNanos(long throttleTimeNanos) {
        snapshotMetrics.createThrottleDurationCounter().incrementBy(throttleTimeNanos, metricAttributes);
        snapshotRateLimitingTimeInNanos.inc(throttleTimeNanos);
    }

    public void incrementRestoreRateLimitingTimeInNanos(long throttleTimeNanos) {
        snapshotMetrics.restoreThrottleDurationCounter().incrementBy(throttleTimeNanos, metricAttributes);
        restoreRateLimitingTimeInNanos.inc(throttleTimeNanos);
    }

    public void incrementCountersForPartUpload(long partSizeInBytes, long partWriteTimeMillis) {
        snapshotMetrics.bytesUploadedCounter().incrementBy(partSizeInBytes, metricAttributes);
        snapshotMetrics.uploadDurationCounter().incrementBy(partWriteTimeMillis, metricAttributes);
        numberOfBytesUploaded.inc(partSizeInBytes);
        uploadTimeInMillis.inc(partWriteTimeMillis);
    }

    public void incrementNumberOfBlobsUploaded() {
        snapshotMetrics.blobsUploadedCounter().incrementBy(1, metricAttributes);
        numberOfBlobsUploaded.inc();
    }

    public void shardSnapshotStarted() {
        snapshotMetrics.shardsStartedCounter().incrementBy(1, metricAttributes);
        numberOfShardSnapshotsStarted.inc();
        shardSnapshotsInProgress.inc();
    }

    public void shardSnapshotCompleted(IndexShardSnapshotStatus status) {
        final Map<String, Object> attrsWithStage = Maps.copyMapWithAddedEntry(metricAttributes, "stage", status.getStage().name());
        snapshotMetrics.shardsCompletedCounter().incrementBy(1, attrsWithStage);
        snapshotMetrics.shardsDurationHistogram().record(status.getTotalTimeMillis() / 1_000d, attrsWithStage);
        numberOfShardSnapshotsCompleted.inc();
        shardSnapshotsInProgress.dec();
    }

    public void incrementUploadReadTime(long readTimeInMillis) {
        snapshotMetrics.uploadReadDurationCounter().incrementBy(readTimeInMillis, metricAttributes);
        uploadReadTimeInNanos.inc(readTimeInMillis);
    }

    public LongWithAttributes getShardSnapshotsInProgress() {
        return new LongWithAttributes(shardSnapshotsInProgress.count(), metricAttributes);
    }

    public RepositoriesStats.SnapshotStats getSnapshotStats() {
        return new RepositoriesStats.SnapshotStats(
            numberOfShardSnapshotsStarted.count(),
            numberOfShardSnapshotsCompleted.count(),
            shardSnapshotsInProgress.count(),
            restoreRateLimitingTimeInNanos.count(),
            snapshotRateLimitingTimeInNanos.count(),
            numberOfBlobsUploaded.count(),
            numberOfBytesUploaded.count(),
            uploadTimeInMillis.count(),
            uploadReadTimeInNanos.count()
        );
    }
}
