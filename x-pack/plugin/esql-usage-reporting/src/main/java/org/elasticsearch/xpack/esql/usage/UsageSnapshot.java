/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome;

import java.time.Instant;
import java.util.Map;

/**
 * What {@link UsageAccumulator#drain} returns: the CPU time summed per {@link Outcome} since the previous drain, and the instant
 * of the drain. Immutable. {@link #isEmpty()} matters because nothing is sent for a quiet period: no record is built for an outcome
 * without usage.
 */
public record UsageSnapshot(Instant drainTime, Map<Outcome, CpuComponents> byOutcome) {

    public UsageSnapshot {
        byOutcome = Map.copyOf(byOutcome);
    }

    /** True when no outcome has a non-zero total, so there is nothing to report. */
    public boolean isEmpty() {
        return byOutcome.values().stream().allMatch(CpuComponents::isZero);
    }
}
