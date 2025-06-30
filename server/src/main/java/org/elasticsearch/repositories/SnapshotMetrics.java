/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public record SnapshotMetrics(
    LongCounter snapshotsShardsStartedCounter,
    LongCounter snapshotsShardsCompletedCounter,
    LongGauge snapshotShardsInProgressGauge,
    DoubleHistogram snapshotShardsDurationHistogram,
    LongCounter snapshotBlobsUploadedCounter,
    LongCounter snapshotBytesUploadedCounter,
    LongCounter snapshotUploadDurationCounter,
    LongCounter snapshotUploadReadDurationCounter,
    LongCounter snapshotCreateThrottleDurationCounter,
    LongCounter snapshotRestoreThrottleDurationCounter
) {

    public static final SnapshotMetrics NOOP = new SnapshotMetrics(MeterRegistry.NOOP, List::of);

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

    public SnapshotMetrics(MeterRegistry meterRegistry, Supplier<Collection<LongWithAttributes>> shardSnapshotsInProgressObserver) {
        this(
            meterRegistry.registerLongCounter(SNAPSHOT_SHARDS_STARTED, "shard snapshots started", "unit"),
            meterRegistry.registerLongCounter(SNAPSHOT_SHARDS_COMPLETED, "shard snapshots completed", "unit"),
            meterRegistry.registerLongsGauge(
                SNAPSHOT_SHARDS_IN_PROGRESS,
                "shard snapshots in progress",
                "unit",
                shardSnapshotsInProgressObserver
            ),
            meterRegistry.registerDoubleHistogram(SNAPSHOT_SHARDS_DURATION, "shard snapshots duration", "s"),
            meterRegistry.registerLongCounter(SNAPSHOT_BLOBS_UPLOADED, "snapshot blobs uploaded", "unit"),
            meterRegistry.registerLongCounter(SNAPSHOT_BYTES_UPLOADED, "snapshot bytes uploaded", "bytes"),
            meterRegistry.registerLongCounter(SNAPSHOT_UPLOAD_DURATION, "snapshot upload duration", "ns"),
            meterRegistry.registerLongCounter(SNAPSHOT_UPLOAD_READ_DURATION, "time spent in read() calls when snapshotting", "ns"),
            meterRegistry.registerLongCounter(SNAPSHOT_CREATE_THROTTLE_DURATION, "time throttled in snapshot create", "bytes"),
            meterRegistry.registerLongCounter(SNAPSHOT_RESTORE_THROTTLE_DURATION, "time throttled in snapshot restore", "bytes")
        );
    }

    public static Map<String, Object> createAttributesMap(RepositoryMetadata meta) {
        return Map.of("repo_type", meta.type(), "repo_name", meta.name());
    }
}
