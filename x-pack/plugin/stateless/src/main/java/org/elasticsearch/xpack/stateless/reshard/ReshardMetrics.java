/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

// We use seconds rather than milliseconds due to the limitations of the default bucket boundaries
// see https://www.elastic.co/docs/reference/apm/agents/java/config-metrics#config-custom-metrics-histogram-boundaries
public record ReshardMetrics(
    LongCounter reshardStartedCounter,
    LongHistogram reshardTargetShardCountHistogram,
    LongHistogram indexingBlockedDurationHistogram,
    DoubleHistogram targetCloneDurationHistogram,
    LongHistogram targetHandoffDurationHistogram,
    LongHistogram targetSplitDurationHistogram,
    MeterRegistry meterRegistry
) {

    public static ReshardMetrics NOOP = new ReshardMetrics(MeterRegistry.NOOP);

    public static final String RESHARD_COUNT = "es.reshard.total";
    public static final String RESHARD_TARGET_SHARD_COUNT = "es.reshard.target_shards.total";
    public static final String RESHARD_INDEXING_BLOCKED_DURATION = "es.reshard.indexing_blocked_time.histogram";
    public static final String RESHARD_TARGET_CLONE_DURATION = "es.reshard.target.clone.duration.histogram";
    public static final String RESHARD_TARGET_HANDOFF_DURATION = "es.reshard.target.handoff.duration.histogram";
    public static final String RESHARD_TARGET_SPLIT_DURATION = "es.reshard.target.split.duration.histogram";

    public ReshardMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongCounter(RESHARD_COUNT, "reshard operations", "count"),
            meterRegistry.registerLongHistogram(RESHARD_TARGET_SHARD_COUNT, "target shard count per reshard operation", "count"),
            meterRegistry.registerLongHistogram(RESHARD_INDEXING_BLOCKED_DURATION, "indexing blocked duration", "ms"),
            meterRegistry.registerDoubleHistogram(RESHARD_TARGET_CLONE_DURATION, "reshard target clone duration", "s"),
            meterRegistry.registerLongHistogram(RESHARD_TARGET_HANDOFF_DURATION, "reshard target handoff duration", "ms"),
            meterRegistry.registerLongHistogram(RESHARD_TARGET_SPLIT_DURATION, "reshard target split duration", "ms"),
            meterRegistry
        );
    }
}
