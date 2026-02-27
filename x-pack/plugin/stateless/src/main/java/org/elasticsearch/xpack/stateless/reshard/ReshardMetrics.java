/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
