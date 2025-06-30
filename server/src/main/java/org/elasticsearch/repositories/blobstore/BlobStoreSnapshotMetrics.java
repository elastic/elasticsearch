/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.metrics.CounterMetric;
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
    private final CounterMetric uploadTimeInNanos = new CounterMetric();
    private final CounterMetric uploadReadTimeInNanos = new CounterMetric();
    private final CounterMetric numberOfShardSnapshotsStarted = new CounterMetric();
    private final CounterMetric numberOfShardSnapshotsCompleted = new CounterMetric();
    private final Map<String, Object> metricAttributes;

    public BlobStoreSnapshotMetrics(RepositoryMetadata repositoryMetadata, SnapshotMetrics snapshotMetrics) {
        this.snapshotMetrics = snapshotMetrics;
        metricAttributes = SnapshotMetrics.createAttributesMap(repositoryMetadata);
    }

    public void incrementSnapshotRateLimitingTimeInNanos(long throttleTimeNanos) {
        snapshotMetrics.snapshotCreateThrottleDurationCounter().incrementBy(throttleTimeNanos);
        snapshotRateLimitingTimeInNanos.inc(throttleTimeNanos);
    }

    public long snapshotRateLimitingTimeInNanos() {
        return snapshotRateLimitingTimeInNanos.count();
    }

    public void incrementRestoreRateLimitingTimeInNanos(long throttleTimeNanos) {
        snapshotMetrics.snapshotRestoreThrottleDurationCounter().incrementBy(throttleTimeNanos);
        restoreRateLimitingTimeInNanos.inc(throttleTimeNanos);
    }

    public long restoreRateLimitingTimeInNanos() {
        return restoreRateLimitingTimeInNanos.count();
    }

    public void incrementCountersForPartUpload(long partSizeInBytes, long partWriteTimeNanos) {
        snapshotMetrics.snapshotBytesUploadedCounter().incrementBy(partSizeInBytes);
        snapshotMetrics.snapshotUploadDurationCounter().incrementBy(partWriteTimeNanos);
        numberOfBytesUploaded.inc(partSizeInBytes);
        uploadTimeInNanos.inc(partWriteTimeNanos);
    }

    public void incrementNumberOfBlobsUploaded() {
        snapshotMetrics.snapshotBlobsUploadedCounter().increment();
        numberOfBlobsUploaded.inc();
    }

    public void shardSnapshotStarted() {
        snapshotMetrics.snapshotsShardsStartedCounter().increment();
        numberOfShardSnapshotsStarted.inc();
        shardSnapshotsInProgress.inc();
    }

    public void shardSnapshotCompleted(long durationInMillis) {
        snapshotMetrics.snapshotsShardsCompletedCounter().increment();
        snapshotMetrics.snapshotShardsDurationHistogram().record(durationInMillis / 1_000f);
        numberOfShardSnapshotsCompleted.inc();
        shardSnapshotsInProgress.dec();
    }

    public void incrementUploadReadTime(long readTimeInNanos) {
        snapshotMetrics.snapshotUploadReadDurationCounter().incrementBy(readTimeInNanos);
        uploadReadTimeInNanos.inc(readTimeInNanos);
    }

    public LongWithAttributes getShardSnapshotsInProgress() {
        return new LongWithAttributes(shardSnapshotsInProgress.count(), metricAttributes);
    }
}
