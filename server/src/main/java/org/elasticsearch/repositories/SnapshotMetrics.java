/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

public record SnapshotMetrics(
    LongCounter snapshotsStartedCounter,
    LongCounter snapshotsCompletedCounter,
    DoubleHistogram snapshotsDurationHistogram,
    LongCounter snapshotsShardsStartedCounter,
    LongCounter snapshotsShardsCompletedCounter,
    DoubleHistogram snapshotShardsDurationHistogram,
    LongCounter snapshotBlobsUploadedCounter,
    LongCounter snapshotBytesUploadedCounter,
    LongCounter snapshotUploadDurationCounter,
    LongCounter snapshotUploadReadDurationCounter,
    LongCounter snapshotCreateThrottleDurationCounter,
    LongCounter snapshotRestoreThrottleDurationCounter,
    MeterRegistry meterRegistry
) {

    public static final SnapshotMetrics NOOP = new SnapshotMetrics(MeterRegistry.NOOP);

    public static final String SNAPSHOTS_STARTED = "es.repositories.snapshots.started.total";
    public static final String SNAPSHOTS_COMPLETED = "es.repositories.snapshots.completed.total";
    public static final String SNAPSHOTS_IN_PROGRESS = "es.repositories.snapshots.current";
    public static final String SNAPSHOT_DURATION = "es.repositories.snapshots.duration.histogram";
    public static final String SNAPSHOT_SHARDS_STARTED = "es.repositories.snapshots.shards.started.total";
    public static final String SNAPSHOT_SHARDS_COMPLETED = "es.repositories.snapshots.shards.completed.total";
    public static final String SNAPSHOT_SHARDS_IN_PROGRESS = "es.repositories.snapshots.shards.current";
    public static final String SNAPSHOT_SHARDS_DURATION = "es.repositories.snapshots.shards.duration.histogram";
    public static final String SNAPSHOT_BLOBS_UPLOADED = "es.repositories.snapshots.blobs.uploaded.total";
    public static final String SNAPSHOT_BYTES_UPLOADED = "es.repositories.snapshots.upload.bytes.total";
    public static final String SNAPSHOT_UPLOAD_DURATION = "es.repositories.snapshots.upload.upload_time.total";
    public static final String SNAPSHOT_UPLOAD_READ_DURATION = "es.repositories.snapshots.upload.read_time.total";
    public static final String SNAPSHOT_CREATE_THROTTLE_DURATION = "es.repositories.snapshots.create_throttling.time.total";
    public static final String SNAPSHOT_RESTORE_THROTTLE_DURATION = "es.repositories.snapshots.restore_throttling.time.total";

    public SnapshotMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongCounter(SNAPSHOTS_STARTED, "snapshots started", "unit"),
            meterRegistry.registerLongCounter(SNAPSHOTS_COMPLETED, "snapshots completed", "unit"),
            meterRegistry.registerDoubleHistogram(SNAPSHOT_DURATION, "snapshots duration", "s"),
            meterRegistry.registerLongCounter(SNAPSHOT_SHARDS_STARTED, "shard snapshots started", "unit"),
            meterRegistry.registerLongCounter(SNAPSHOT_SHARDS_COMPLETED, "shard snapshots completed", "unit"),
            meterRegistry.registerDoubleHistogram(SNAPSHOT_SHARDS_DURATION, "shard snapshots duration", "s"),
            meterRegistry.registerLongCounter(SNAPSHOT_BLOBS_UPLOADED, "snapshot blobs uploaded", "unit"),
            meterRegistry.registerLongCounter(SNAPSHOT_BYTES_UPLOADED, "snapshot bytes uploaded", "bytes"),
            meterRegistry.registerLongCounter(SNAPSHOT_UPLOAD_DURATION, "snapshot upload duration", "ns"),
            meterRegistry.registerLongCounter(SNAPSHOT_UPLOAD_READ_DURATION, "time spent in read() calls when snapshotting", "ns"),
            meterRegistry.registerLongCounter(SNAPSHOT_CREATE_THROTTLE_DURATION, "time throttled in snapshot create", "bytes"),
            meterRegistry.registerLongCounter(SNAPSHOT_RESTORE_THROTTLE_DURATION, "time throttled in snapshot restore", "bytes"),
            meterRegistry
        );
    }

    public void createSnapshotShardsInProgressMetric(Supplier<Collection<LongWithAttributes>> shardSnapshotsInProgressObserver) {
        meterRegistry.registerLongsGauge(
            SNAPSHOT_SHARDS_IN_PROGRESS,
            "shard snapshots in progress",
            "unit",
            shardSnapshotsInProgressObserver
        );
    }

    public void createSnapshotsInProgressMetric(Supplier<Collection<LongWithAttributes>> snapshotsInProgressObserver) {
        meterRegistry.registerLongsGauge(SNAPSHOTS_IN_PROGRESS, "snapshots in progress", "unit", snapshotsInProgressObserver);
    }

    public static Map<String, Object> createAttributesMap(ProjectId projectId, RepositoryMetadata meta) {
        return Map.of("project_id", projectId.id(), "repo_type", meta.type(), "repo_name", meta.name());
    }
}
