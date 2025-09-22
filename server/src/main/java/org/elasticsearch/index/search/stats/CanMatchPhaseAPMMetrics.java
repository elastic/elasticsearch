/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class CanMatchPhaseAPMMetrics extends SearchPhaseAPMMetrics {

    public static final String CAN_MATCH_SEARCH_PHASE_PER_SHARD_METRIC = "es.search.shards.phases.can_match.duration.histogram";
    public static final String CAN_MATCH_SEARCH_PHASE_COORDINATING_NODE_METRIC =
        "es.search.coordinating_node.phases.can_match.duration.histogram";

    public static final CanMatchPhaseAPMMetrics NOOP = new CanMatchPhaseAPMMetrics(MeterRegistry.NOOP) {
        @Override
        protected void recordPhaseLatency(LongHistogram histogramMetric, long tookInNanos) {
            // noop
        }
    };

    private final LongHistogram canMatchPhasePerShardMetric;
    private final LongHistogram canMatchPhaseCoordinatingNodeMetric;

    public CanMatchPhaseAPMMetrics(MeterRegistry meterRegistry) {
        this.canMatchPhasePerShardMetric = meterRegistry.registerLongHistogram(
            CAN_MATCH_SEARCH_PHASE_PER_SHARD_METRIC,
            "Can match phase execution times at the shard level, expressed as a histogram",
            "ms"
        );
        this.canMatchPhaseCoordinatingNodeMetric = meterRegistry.registerLongHistogram(
            CAN_MATCH_SEARCH_PHASE_COORDINATING_NODE_METRIC,
            "Can match phase execution times at the coordinating node level, expressed as a histogram",
            "ms"
        );
    }

    public void onCanMatchPhasePerShard(long tookInNanos) {
        recordPhaseLatency(canMatchPhasePerShardMetric, tookInNanos);
    }

    public void onCanMatchPhaseCoordinatingNode(long tookInNanos) {
        recordPhaseLatency(canMatchPhaseCoordinatingNodeMetric, tookInNanos);
    }
}
