/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/// The singleton registration point for the metrics recorded by [BalancedShardsAllocator]. There are
/// multiple instances of [BalancedShardsAllocator] so we can't register the metrics in there.
public class BalancedShardsAllocatorMetrics {

    static final String CANNOT_REMAIN_MOVE_METRIC = "es.allocator.shards.cannot_remain_moves.total";

    public static final BalancedShardsAllocatorMetrics NOOP = new BalancedShardsAllocatorMetrics(MeterRegistry.NOOP);

    private final LongCounter cannotRemainMoveCounter;

    public BalancedShardsAllocatorMetrics(MeterRegistry meterRegistry) {
        this.cannotRemainMoveCounter = meterRegistry.registerLongCounter(
            CANNOT_REMAIN_MOVE_METRIC,
            "Total number of shard moves triggered by a non-YES canRemain decision",
            "unit"
        );
    }

    public void incrementCannotRemainMoveCounter(Map<String, Object> attributes) {
        this.cannotRemainMoveCounter.incrementBy(1, attributes);
    }
}
