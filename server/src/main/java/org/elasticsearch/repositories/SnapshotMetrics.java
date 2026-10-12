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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.snapshots.CachingSnapshotAndShardByStateMetricsService;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongAsyncGauge;
import org.elasticsearch.telemetry.metric.LongAsyncMeasurement;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SnapshotMetrics extends AbstractLifecycleComponent {

    public static final SnapshotMetrics NOOP = new SnapshotMetrics(MeterRegistry.NOOP, m -> {}, m -> {}, m -> {}, m -> {});

    public static final String SNAPSHOTS_STARTED = "es.repositories.snapshots.started.total";
    public static final String SNAPSHOTS_COMPLETED = "es.repositories.snapshots.completed.total";
    public static final String SNAPSHOTS_BY_STATE = "es.repositories.snapshots.by_state.current";
    public static final String SNAPSHOT_DURATION = "es.repositories.snapshots.duration.histogram";
    public static final String SNAPSHOT_SHARDS_STARTED = "es.repositories.snapshots.shards.started.total";
    public static final String SNAPSHOT_SHARDS_COMPLETED = "es.repositories.snapshots.shards.completed.total";
    public static final String SNAPSHOT_SHARDS_IN_PROGRESS = "es.repositories.snapshots.shards.current";
    public static final String SNAPSHOT_SHARDS_BY_STATE = "es.repositories.snapshots.shards.by_state.current";
    public static final String SNAPSHOT_SHARDS_DURATION = "es.repositories.snapshots.shards.duration.histogram";
    public static final String SNAPSHOT_SHARDS_QUEUE_TIME = "es.repositories.snapshots.shards.queue_time.histogram";
    public static final String SNAPSHOT_SHARDS_UNSUCCESSFUL = "es.repositories.snapshots.shards.unsuccessful.total";
    public static final String SNAPSHOT_SHARDS_UNSUCCESSFUL_HISTOGRAM = "es.repositories.snapshots.shards.unsuccessful.histogram";
    public static final String SNAPSHOT_BLOBS_UPLOADED = "es.repositories.snapshots.blobs.uploaded.total";
    public static final String SNAPSHOT_BYTES_UPLOADED = "es.repositories.snapshots.upload.bytes.total";
    public static final String SNAPSHOT_UPLOAD_DURATION = "es.repositories.snapshots.upload.upload_time.total";
    public static final String SNAPSHOT_UPLOAD_READ_DURATION = "es.repositories.snapshots.upload.read_time.total";
    public static final String SNAPSHOT_CREATE_THROTTLE_DURATION = "es.repositories.snapshots.create_throttling.time.total";
    public static final String SNAPSHOT_RESTORE_THROTTLE_DURATION = "es.repositories.snapshots.restore_throttling.time.total";
    public static final String SNAPSHOT_SHARDS_WAITING_LATENCY = "es.repositories.snapshots.shards.waiting.latency.time.current";

    private final LongCounter snapshotsStartedCounter;
    private final LongCounter snapshotsCompletedCounter;
    private final DoubleHistogram snapshotsDurationHistogram;
    private final LongCounter shardsStartedCounter;
    private final LongCounter shardsCompletedCounter;
    private final DoubleHistogram shardsDurationHistogram;
    private final DoubleHistogram shardsQueueTimeHistogram;
    private final LongCounter shardsUnsuccessfulCounter;
    private final LongHistogram shardsUnsuccessfulHistogram;
    private final LongCounter blobsUploadedCounter;
    private final LongCounter bytesUploadedCounter;
    private final LongCounter uploadDurationCounter;
    private final LongCounter uploadReadDurationCounter;
    private final LongCounter createThrottleDurationCounter;
    private final LongCounter restoreThrottleDurationCounter;
    private final List<LongAsyncGauge> asyncGauges;
    private final MeterRegistry meterRegistry;
    private final Consumer<LongAsyncMeasurement> shardSnapshotsInProgressObserver;
    private final Consumer<LongAsyncMeasurement> shardSnapshotsByStatusObserver;
    private final Consumer<LongAsyncMeasurement> snapshotsByStatusObserver;
    private final Consumer<LongAsyncMeasurement> longestWaitingTimeMillisObserver;

    public SnapshotMetrics(
        MeterRegistry meterRegistry,
        CachingSnapshotAndShardByStateMetricsService cachingSnapshotAndShardByStateMetricsService,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this(
            meterRegistry,
            measurement -> repositoriesServiceSupplier.get().recordShardSnapshotsInProgress(measurement),
            cachingSnapshotAndShardByStateMetricsService::recordShardsByState,
            cachingSnapshotAndShardByStateMetricsService::recordSnapshotsByState,
            cachingSnapshotAndShardByStateMetricsService::recordLongestWaitingTimeMillis
        );
    }

    public SnapshotMetrics(
        MeterRegistry meterRegistry,
        Consumer<LongAsyncMeasurement> shardSnapshotsInProgressObserver,
        Consumer<LongAsyncMeasurement> shardSnapshotsByStatusObserver,
        Consumer<LongAsyncMeasurement> snapshotsByStatusObserver,
        Consumer<LongAsyncMeasurement> longestWaitingTimeMillisObserver
    ) {
        this.shardSnapshotsInProgressObserver = shardSnapshotsInProgressObserver;
        this.shardSnapshotsByStatusObserver = shardSnapshotsByStatusObserver;
        this.snapshotsByStatusObserver = snapshotsByStatusObserver;
        this.longestWaitingTimeMillisObserver = longestWaitingTimeMillisObserver;
        this.snapshotsStartedCounter = meterRegistry.registerLongCounter(SNAPSHOTS_STARTED, "snapshots started", "unit");
        this.snapshotsCompletedCounter = meterRegistry.registerLongCounter(SNAPSHOTS_COMPLETED, "snapshots completed", "unit");
        // We use seconds rather than milliseconds due to the limitations of the default bucket boundaries
        // see https://www.elastic.co/docs/reference/apm/agents/java/config-metrics#config-custom-metrics-histogram-boundaries
        this.snapshotsDurationHistogram = meterRegistry.registerDoubleHistogram(SNAPSHOT_DURATION, "snapshots duration", "s");
        this.shardsStartedCounter = meterRegistry.registerLongCounter(SNAPSHOT_SHARDS_STARTED, "shard snapshots started", "unit");
        this.shardsCompletedCounter = meterRegistry.registerLongCounter(SNAPSHOT_SHARDS_COMPLETED, "shard snapshots completed", "unit");
        // We use seconds rather than milliseconds due to the limitations of the default bucket boundaries
        // see https://www.elastic.co/docs/reference/apm/agents/java/config-metrics#config-custom-metrics-histogram-boundaries
        this.shardsDurationHistogram = meterRegistry.registerDoubleHistogram(SNAPSHOT_SHARDS_DURATION, "shard snapshots duration", "s");
        this.shardsQueueTimeHistogram = meterRegistry.registerDoubleHistogram(
            SNAPSHOT_SHARDS_QUEUE_TIME,
            "shard snapshots queue time",
            "s"
        );
        this.shardsUnsuccessfulCounter = meterRegistry.registerLongCounter(
            SNAPSHOT_SHARDS_UNSUCCESSFUL,
            "unsuccessful shard snapshots",
            "unit"
        );
        this.shardsUnsuccessfulHistogram = meterRegistry.registerLongHistogram(
            SNAPSHOT_SHARDS_UNSUCCESSFUL_HISTOGRAM,
            "unsuccessful shard snapshots per snapshot",
            "unit",
            // Boundaries are chosen to:
            // - give 0 its own bucket so clean snapshots are trivially separated from affected ones;
            // - provide fine granularity at low counts (1–10) where individual shard failures are actionable;
            // - extend well past the default per-index shard limit (1 024) because a snapshot can include many
            // indices, so the total unsuccessful count is bounded only by the cluster-wide shard count
            // (cluster.max_shards_per_node × data nodes, default 1 000/node).
            List.of(0L, 1L, 2L, 5L, 10L, 25L, 50L, 100L, 250L, 500L, 1000L, 2500L, 5000L, 10_000L, 50_000L)
        );
        this.blobsUploadedCounter = meterRegistry.registerLongCounter(SNAPSHOT_BLOBS_UPLOADED, "snapshot blobs uploaded", "unit");
        this.bytesUploadedCounter = meterRegistry.registerLongCounter(SNAPSHOT_BYTES_UPLOADED, "snapshot bytes uploaded", "bytes");
        this.uploadDurationCounter = meterRegistry.registerLongCounter(SNAPSHOT_UPLOAD_DURATION, "snapshot upload duration", "ms");
        this.uploadReadDurationCounter = meterRegistry.registerLongCounter(
            SNAPSHOT_UPLOAD_READ_DURATION,
            "time spent in read() calls when snapshotting",
            "ms"
        );
        this.createThrottleDurationCounter = meterRegistry.registerLongCounter(
            SNAPSHOT_CREATE_THROTTLE_DURATION,
            "time throttled in snapshot create",
            "ns"
        );
        this.restoreThrottleDurationCounter = meterRegistry.registerLongCounter(
            SNAPSHOT_RESTORE_THROTTLE_DURATION,
            "time throttled in snapshot restore",
            "ns"
        );
        this.asyncGauges = new ArrayList<>();
        this.meterRegistry = meterRegistry;
    }

    @FixForMultiProject(description = "When multi-project arrives we should add project ID to the labels")
    public static Map<String, Object> createAttributesMap(ProjectId projectId, RepositoryMetadata meta) {
        return Map.of("repo_type", meta.type(), "repo_name", meta.name());
    }

    public LongCounter snapshotsStartedCounter() {
        return snapshotsStartedCounter;
    }

    public LongCounter snapshotsCompletedCounter() {
        return snapshotsCompletedCounter;
    }

    public DoubleHistogram snapshotsDurationHistogram() {
        return snapshotsDurationHistogram;
    }

    public LongCounter shardsStartedCounter() {
        return shardsStartedCounter;
    }

    public LongCounter shardsCompletedCounter() {
        return shardsCompletedCounter;
    }

    public DoubleHistogram shardsDurationHistogram() {
        return shardsDurationHistogram;
    }

    public DoubleHistogram shardsQueueTimeHistogram() {
        return shardsQueueTimeHistogram;
    }

    public LongCounter shardsUnsuccessfulCounter() {
        return shardsUnsuccessfulCounter;
    }

    public LongHistogram shardsUnsuccessfulHistogram() {
        return shardsUnsuccessfulHistogram;
    }

    public LongCounter blobsUploadedCounter() {
        return blobsUploadedCounter;
    }

    public LongCounter bytesUploadedCounter() {
        return bytesUploadedCounter;
    }

    public LongCounter uploadDurationCounter() {
        return uploadDurationCounter;
    }

    public LongCounter uploadReadDurationCounter() {
        return uploadReadDurationCounter;
    }

    public LongCounter createThrottleDurationCounter() {
        return createThrottleDurationCounter;
    }

    public LongCounter restoreThrottleDurationCounter() {
        return restoreThrottleDurationCounter;
    }

    @Override
    protected void doStart() {
        asyncGauges.add(
            meterRegistry.registerLongAsyncGauge(
                SNAPSHOT_SHARDS_IN_PROGRESS,
                "shard snapshots in progress",
                "unit",
                shardSnapshotsInProgressObserver
            )
        );
        asyncGauges.add(
            meterRegistry.registerLongAsyncGauge(
                SNAPSHOT_SHARDS_BY_STATE,
                "snapshotting shards by state",
                "unit",
                shardSnapshotsByStatusObserver
            )
        );
        asyncGauges.add(meterRegistry.registerLongAsyncGauge(SNAPSHOTS_BY_STATE, "snapshots by state", "unit", snapshotsByStatusObserver));
        asyncGauges.add(
            meterRegistry.registerLongAsyncGauge(
                SNAPSHOT_SHARDS_WAITING_LATENCY,
                "current longest time any shard snapshot has been WAITING (with the current master)",
                "milliseconds",
                longestWaitingTimeMillisObserver
            )
        );
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {
        Releasables.close(asyncGauges.stream().map(g -> (Releasable) g::close).toList());
    }
}
