/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.xpack.core.esql.QueryMetricsListener;

import java.util.Map;

/**
 * The CPU time of queries over external sources, split in the four parts that are counted. The reported quantity is their sum,
 * {@link #totalNanos()}: planning, split discovery, execution (compute drivers) and reading data, all without time spent waiting
 * for I/O.
 * <p>
 * The values are nanoseconds here and become milliseconds only when a {@link UsageRecord} is built. The parts are kept apart
 * rather than summed at once so that a future change in what is counted does not touch the hot path, and they are additive by
 * design: the components of one query may land in different drains of the {@link UsageAccumulator}, and sums of sums stay exact.
 */
public record CpuComponents(long planningNanos, long splitDiscoveryNanos, long executionNanos, long readNanos) {

    public static final CpuComponents ZERO = new CpuComponents(0, 0, 0, 0);

    /**
     * Reads the counted parts out of the metrics of {@link QueryMetricsListener}. A key that is absent counts as 0, and the other
     * keys of the map (wall-clock times, bytes read) are not counted and are ignored.
     */
    public static CpuComponents fromMetrics(Map<String, Long> metrics) {
        return new CpuComponents(
            metrics.getOrDefault(QueryMetricsListener.PLANNING_CPU_NANOS, 0L),
            metrics.getOrDefault(QueryMetricsListener.SPLIT_DISCOVERY_CPU_NANOS, 0L),
            metrics.getOrDefault(QueryMetricsListener.CPU_NANOS, 0L),
            metrics.getOrDefault(QueryMetricsListener.READ_CPU_NANOS, 0L)
        );
    }

    public CpuComponents plus(CpuComponents other) {
        return new CpuComponents(
            planningNanos + other.planningNanos,
            splitDiscoveryNanos + other.splitDiscoveryNanos,
            executionNanos + other.executionNanos,
            readNanos + other.readNanos
        );
    }

    /** The reported quantity before its conversion to milliseconds. */
    public long totalNanos() {
        return planningNanos + splitDiscoveryNanos + executionNanos + readNanos;
    }

    public boolean isZero() {
        return totalNanos() == 0;
    }
}
