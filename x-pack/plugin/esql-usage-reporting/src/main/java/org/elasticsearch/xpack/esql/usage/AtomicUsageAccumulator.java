/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The real {@link UsageAccumulator}: one sum per {@link Outcome} and per CPU component, each updated with atomic operations only.
 * <p>
 * Every sum is a plain {@code long} cell of an {@link AtomicLongArray} rather than a {@link java.util.concurrent.atomic.LongAdder}.
 * A {@code LongAdder} spreads contended updates over several cells, which is what makes it fast under heavy contention, but its
 * {@code sumThenReset} is only specified as a best-effort snapshot under concurrent updates, and the exactness of the drain is the
 * one property that matters here: an addend that is lost is usage that is never billed, and one that is returned twice is usage
 * billed twice. Each query adds to the cells once, when it completes, so contention is low and the relief of a {@code LongAdder}
 * is not needed, while {@link AtomicLongArray#getAndSet} gives a drain whose exactness is guaranteed by its specification.
 * <p>
 * Concurrent {@link #record} and {@link #drain} calls are safe. The cells are reset one at a time, so the components of one query
 * that is recorded while a drain runs may land in two different drains. That is acceptable: each addend is counted exactly once,
 * and the sums are additive.
 */
public final class AtomicUsageAccumulator implements UsageAccumulator {

    private static final Outcome[] OUTCOMES = Outcome.values();
    private static final int COMPONENTS = 4;
    private static final int PLANNING = 0;
    private static final int SPLIT_DISCOVERY = 1;
    private static final int EXECUTION = 2;
    private static final int READ = 3;

    /** The cell of component {@code c} of outcome {@code o} is {@code o.ordinal() * COMPONENTS + c}. */
    private final AtomicLongArray cells = new AtomicLongArray(OUTCOMES.length * COMPONENTS);

    @Override
    public void record(Outcome outcome, Map<String, Long> metrics) {
        // One small record per completed query: the hot path stays free of locks and I/O, and the key names stay in CpuComponents.
        CpuComponents components = CpuComponents.fromMetrics(metrics);
        int base = outcome.ordinal() * COMPONENTS;
        add(base + PLANNING, components.planningNanos());
        add(base + SPLIT_DISCOVERY, components.splitDiscoveryNanos());
        add(base + EXECUTION, components.executionNanos());
        add(base + READ, components.readNanos());
    }

    /** A negative value cannot be real CPU time; counting it would lower the bill, so it counts as 0. */
    private void add(int cell, long nanos) {
        if (nanos > 0) {
            cells.addAndGet(cell, nanos);
        }
    }

    /**
     * Outcomes without any recorded usage are left out of the snapshot, as in the snapshot of the no-op accumulator.
     */
    @Override
    public UsageSnapshot drain(Instant drainTime) {
        Map<Outcome, CpuComponents> byOutcome = new EnumMap<>(Outcome.class);
        for (Outcome outcome : OUTCOMES) {
            int base = outcome.ordinal() * COMPONENTS;
            CpuComponents drained = new CpuComponents(
                cells.getAndSet(base + PLANNING, 0),
                cells.getAndSet(base + SPLIT_DISCOVERY, 0),
                cells.getAndSet(base + EXECUTION, 0),
                cells.getAndSet(base + READ, 0)
            );
            if (drained.isZero() == false) {
                byOutcome.put(outcome, drained);
            }
        }
        return new UsageSnapshot(drainTime, byOutcome);
    }
}
