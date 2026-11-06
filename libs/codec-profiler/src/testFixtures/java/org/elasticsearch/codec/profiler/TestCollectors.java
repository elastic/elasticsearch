/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/**
 * Test support for code that records into a {@link ProfileCollector}: collectors with deterministic clocks, a scope
 * that hands every reader one recorder, and a flat view of a snapshot for assertions. None of it is production API.
 */
public final class TestCollectors {

    /** The bytes a detail retains for itself, as reported to its reservations. */
    public static final long DETAIL_BYTES = ProfileCollector.DETAIL_BYTES;
    /** The bytes a detail retains for a counter. */
    public static final long COUNTER_BYTES = ProfileCollector.COUNTER_BYTES;
    /** The bytes a detail retains for a timer without a CPU time companion. */
    public static final long TIMER_BYTES = ProfileCollector.TIMER_BYTES;
    /** The bytes a detail retains for a timer's CPU time companion, reserved with the timer. */
    public static final long CPU_TIME_BYTES = ProfileCollector.CPU_TIME_BYTES;
    /** The most metric identities a collector accepts. */
    public static final int MAX_METRICS = ProfileCollector.MAX_METRICS;

    private TestCollectors() {}

    /** Counters and elapsed time with the system clock, as a frontend builds it. */
    public static ProfileCollector elapsed() {
        return ProfileCollector.create(ProfileCollector.Mode.ELAPSED);
    }

    /** Counters only: no clock is read and no timer is registered. */
    public static ProfileCollector countersOnly() {
        return ProfileCollector.create(ProfileCollector.Mode.COUNTERS);
    }

    /** Counters and elapsed time measured with {@code clock}, which returns monotonic nanoseconds. */
    public static ProfileCollector withClock(LongSupplier clock) {
        return ProfileCollector.create(ProfileCollector.Mode.ELAPSED, clock, null);
    }

    /** Counters, elapsed time with {@code clock} and thread CPU time with {@code cpuClock}, on any platform. */
    public static ProfileCollector withClocks(LongSupplier clock, LongSupplier cpuClock) {
        return ProfileCollector.create(ProfileCollector.Mode.ELAPSED_AND_CPU, clock, cpuClock);
    }

    /** A collector asked for thread CPU time on a platform that cannot measure it: elapsed time only. */
    public static ProfileCollector cpuUnsupported(LongSupplier clock) {
        return ProfileCollector.create(ProfileCollector.Mode.ELAPSED_AND_CPU, clock, null);
    }

    /** Whether {@code collector} still measures thread CPU time. */
    public static boolean measuresThreadCpuTime(ProfileCollector collector) {
        return collector.measuresThreadCpuTime();
    }

    /** A scope whose readers all report to {@code metrics}. */
    public static Scope eager(MetricRecorder metrics) {
        return Scopes.eager(metrics);
    }

    /** Binds a scope whose readers all report to {@code metrics}. */
    public static Scopes.Binding bind(MetricRecorder metrics) {
        return Scopes.bind(Scopes.eager(metrics));
    }

    /** Whether a scope is bound on this thread. */
    public static boolean isBound() {
        return Scopes.isBound();
    }

    /** What {@code counter} has counted so far. */
    public static long value(Counter counter) {
        return counter.value();
    }

    /**
     * The values of {@code snapshot} by {@link Metric.Id#key()}, ordered by key: a counter's count, a timer's elapsed
     * time under its own key and its CPU time, when present, under its companion's key. It drops the definitions and the
     * call counts, so it is for assertions only.
     */
    public static Map<String, Long> toKeyedValues(Snapshot snapshot) {
        final Map<String, Long> values = new TreeMap<>();
        for (Measurement measurement : snapshot.measurements()) {
            switch (measurement) {
                case CounterMeasurement counter -> values.put(counter.id().key(), counter.value());
                case TimerMeasurement timer -> {
                    values.put(timer.id().key(), timer.elapsedNanos());
                    if (timer.cpuNanos().isPresent()) {
                        values.put(timer.definition().cpuTime().id().key(), timer.cpuNanos().getAsLong());
                    }
                }
            }
        }
        return Collections.unmodifiableMap(values);
    }
}
