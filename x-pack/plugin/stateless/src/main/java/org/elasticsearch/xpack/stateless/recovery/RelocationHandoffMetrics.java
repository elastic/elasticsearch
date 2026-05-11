/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

/**
 * Histograms for primary relocation handoff: the round-trip {@code handoffDuration} recorded on the source,
 * and the target-side sub-phases {@code preRecoveryDuration} / {@code readIndexingShardStateDuration} / {@code openEngineDuration}.
 */
public record RelocationHandoffMetrics(
    LongHistogram handoffDuration,
    LongHistogram preRecoveryDuration,
    LongHistogram readIndexingShardStateDuration,
    LongHistogram openEngineDuration
) {

    public static final String HANDOFF_DURATION = "es.primary.relocation.handoff.duration.histogram";
    public static final String PRE_RECOVERY_DURATION = "es.primary.relocation.target.pre_recovery.duration.histogram";
    public static final String READ_INDEXING_SHARD_STATE_DURATION =
        "es.primary.relocation.target.read_indexing_shard_state.duration.histogram";
    public static final String OPEN_ENGINE_DURATION = "es.primary.relocation.target.open_engine.duration.histogram";

    public static final RelocationHandoffMetrics NOOP = from(MeterRegistry.NOOP);

    public static RelocationHandoffMetrics from(MeterRegistry meterRegistry) {
        return new RelocationHandoffMetrics(
            meterRegistry.registerLongHistogram(
                HANDOFF_DURATION,
                "Round-trip duration of the primary relocation handoff context phase, measured on the source",
                "ms"
            ),
            meterRegistry.registerLongHistogram(
                PRE_RECOVERY_DURATION,
                "Time spent in IndexShard#preRecovery during primary relocation handoff on the target",
                "ms"
            ),
            meterRegistry.registerLongHistogram(
                READ_INDEXING_SHARD_STATE_DURATION,
                "Time spent in ObjectStoreService#readIndexingShardState (BCC chain walk) during primary relocation handoff on the target",
                "ms"
            ),
            meterRegistry.registerLongHistogram(
                OPEN_ENGINE_DURATION,
                "Time spent opening the engine (and activating with primary context) during primary relocation handoff on the target",
                "ms"
            )
        );
    }
}
