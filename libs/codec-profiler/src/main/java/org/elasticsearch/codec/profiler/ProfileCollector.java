/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/**
 * The {@link MetricRecorder} that a frontend creates and owns. A {@link Scope} returns it to each reader opened while
 * the scope is bound. A codec sees it only as a {@link MetricRecorder}. The frontend configures it, creates its
 * details and takes its snapshots.
 *
 * <p>A reader keeps the recorder it got when it opened. Work done after the binding is closed, for example the
 * decompression of a block during iteration, thus goes to the collector of the request that opened the reader. This
 * requires that the reader, and every component that keeps the recorder, works only for that request. The module
 * documentation states the ownership rule.
 *
 * <p>A collector, its details and their handles have one writer at a time. Registration, recording and
 * {@link #snapshot()} need exclusive access, or a completed handoff that orders the writes. Take a snapshot between
 * calls: a call still running counts as an unmeasured call, so its timer reports an estimate. A snapshot copies the
 * values, so it does not alias a live counter.
 *
 * <p>A detail collector, created by {@link #detail}, records a part of the work of its parent, for example one field of
 * one segment. Every event on a detail also reaches its parent once, so the parent counts every event once with or
 * without details. A detail timer decides from its own calls which calls to measure. The decisions of the parent are
 * the same as without details. A parent and its details share one CPU state. Details cannot have details.
 */
public final class ProfileCollector implements MetricRecorder {

    /** The maximum number of metric identities in a collector. A CPU time companion counts as one identity. */
    static final int MAX_METRICS = 64;

    /** What a collector measures. Counters are always counted. */
    public enum Mode {
        /** Counters only. No clock is read and no timer is registered, so timers are absent from snapshots. */
        COUNTERS,
        /** Counters and elapsed time. */
        ELAPSED,
        /**
         * Counters, elapsed time, and the thread CPU time of the measured calls of timers that have a CPU time
         * companion. On a platform that cannot measure thread CPU time, this mode is the same as {@link #ELAPSED} and
         * the CPU time is absent.
         */
        ELAPSED_AND_CPU
    }

    /**
     * Receives the bytes that a detail will retain, before the detail allocates them, so that the owner can account for
     * the memory. An exception from {@link #reserve} stops the allocation.
     */
    @FunctionalInterface
    public interface Reservations {
        void reserve(long bytes);
    }

    /** The bytes a detail retains for itself, before any metric: the collector and its empty registry. */
    static final long DETAIL_BYTES = shallowBytes(8, 0, 0) + shallowBytes(8, 0, 2);
    /** The bytes of a registry entry: the entry of the map and the record it holds. */
    private static final long ENTRY_BYTES = shallowBytes(5, 0, 1) + shallowBytes(3, 0, 1);
    /** The bytes a detail retains for a counter. */
    static final long COUNTER_BYTES = ENTRY_BYTES + shallowBytes(1, 1, 0);
    /** The bytes a detail retains for a timer, without its CPU time companion. */
    static final long TIMER_BYTES = ENTRY_BYTES + shallowBytes(5, 9, 4);
    /** The bytes a detail retains for a timer's CPU time companion, which shares the timer. */
    static final long CPU_TIME_BYTES = ENTRY_BYTES;

    private final TreeMap<Metric.Id, Registered> registered = new TreeMap<>();

    /** Null when this collector does not measure time. */
    @Nullable
    private final LongSupplier clock;

    /** Null when this collector does not measure thread CPU time, including when the platform cannot. */
    @Nullable
    private final LongSupplier cpuClock;

    /** Shared by a collector and its details. */
    private final CpuState cpuState;

    /** The collector this one is a detail of, or null for a collector that is not a detail. */
    @Nullable
    private final ProfileCollector parent;

    /** Null for a collector that is not a detail. */
    @Nullable
    private final Reservations reservations;

    private ProfileCollector(
        @Nullable LongSupplier clock,
        @Nullable LongSupplier cpuClock,
        CpuState cpuState,
        @Nullable ProfileCollector parent,
        @Nullable Reservations reservations
    ) {
        assert clock != null || cpuClock == null : "thread CPU time is a companion of elapsed time";
        this.clock = clock;
        this.cpuClock = cpuClock;
        this.cpuState = cpuState;
        this.parent = parent;
        this.reservations = reservations;
    }

    /** A collector for {@code mode} that uses {@link System#nanoTime()} and the platform thread CPU clock. */
    public static ProfileCollector create(Mode mode) {
        return switch (mode) {
            case COUNTERS -> create(mode, null, null);
            case ELAPSED -> create(mode, System::nanoTime, null);
            case ELAPSED_AND_CPU -> create(mode, System::nanoTime, ThreadCpuClock.platform());
        };
    }

    /**
     * A collector with the given clocks, for tests that need deterministic time. A null {@code cpuClock} with
     * {@link Mode#ELAPSED_AND_CPU} simulates a platform that cannot measure thread CPU time.
     */
    static ProfileCollector create(Mode mode, @Nullable LongSupplier clock, @Nullable LongSupplier cpuClock) {
        Objects.requireNonNull(mode, "mode");
        switch (mode) {
            case COUNTERS -> {
                if (clock != null || cpuClock != null) {
                    throw new IllegalArgumentException("a collector of counters reads no clock");
                }
            }
            case ELAPSED -> {
                Objects.requireNonNull(clock, "clock");
                if (cpuClock != null) {
                    throw new IllegalArgumentException("a collector of elapsed time reads no CPU clock");
                }
            }
            case ELAPSED_AND_CPU -> Objects.requireNonNull(clock, "clock");
        }
        return new ProfileCollector(clock, cpuClock, new CpuState(), null, null);
    }

    /**
     * A new detail collector of this collector. A metric registered on the detail is also registered on this
     * collector, and every event recorded on the detail also reaches this collector. The detail uses the clocks and
     * the CPU state of this collector.
     *
     * <p>{@code reservations} receives the bytes of the detail first, then the bytes of each metric that the detail
     * registers, before each allocation. A timer and its CPU time companion are reserved together. When a reservation
     * fails, the detail does not get the metric, but this collector keeps it, because it registers the metric first.
     *
     * @throws IllegalStateException when this collector is itself a detail
     */
    public ProfileCollector detail(Reservations reservations) {
        Objects.requireNonNull(reservations, "reservations");
        if (parent != null) {
            throw new IllegalStateException("a detail cannot have details of its own");
        }
        reservations.reserve(DETAIL_BYTES);
        return new ProfileCollector(clock, cpuClock, cpuState, this, reservations);
    }

    /** Whether this collector measures thread CPU time: the mode asks for it, the platform supports it, and no reading failed. */
    boolean measuresThreadCpuTime() {
        return cpuClock != null && cpuState.failed == false;
    }

    @Override
    public Counter counter(CounterMetric metric) {
        final Registered existing = existing(metric.id(), metric);
        if (existing != null) {
            return existing.counter;
        }
        ensureRoom(metric.id(), 1);
        final Counter parentCounter = parent == null ? null : parent.counter(metric);
        if (reservations != null) {
            reservations.reserve(COUNTER_BYTES);
        }
        final Counter created = new Counter(parentCounter);
        registered.put(key(metric.id()), new Registered(metric, created, null, false));
        return created;
    }

    @Override
    public Timer timer(TimerMetric metric) {
        if (clock == null) {
            return Timer.INERT;
        }
        final Registered existing = existing(metric.id(), metric);
        if (existing != null) {
            return existing.timer;
        }
        final boolean measuresCpu = cpuClock != null && metric.cpuTime() != null;
        if (measuresCpu && existing(metric.cpuTime().id(), metric) != null) {
            throw new IllegalStateException("the CPU time companion of [" + metric.id() + "] is registered without its timer");
        }
        ensureRoom(metric.id(), measuresCpu ? 2 : 1);
        final Timer parentTimer = parent == null ? null : parent.timer(metric);
        if (reservations != null) {
            reservations.reserve(TIMER_BYTES + (measuresCpu ? CPU_TIME_BYTES : 0));
        }
        final Timer created = new Timer(clock, metric.policy(), parentTimer, measuresCpu ? cpuClock : null, measuresCpu ? cpuState : null);
        registered.put(key(metric.id()), new Registered(metric, null, created, false));
        if (measuresCpu) {
            registered.put(key(metric.cpuTime().id()), new Registered(metric, null, created, true));
        }
        return created;
    }

    /**
     * The identity object to register {@code id} under. A detail reuses the identity object of its parent, so that the
     * details of one parent share one object for each identity.
     */
    private Metric.Id key(Metric.Id id) {
        if (parent == null) {
            return id;
        }
        final Metric.Id shared = parent.registered.ceilingKey(id);
        return id.equals(shared) ? shared : id;
    }

    /**
     * The entry registered under {@code id}, or null when there is none.
     *
     * @throws IllegalStateException when the entry has a definition other than {@code metric}
     */
    @Nullable
    private Registered existing(Metric.Id id, Metric metric) {
        final Registered existing = registered.get(id);
        if (existing != null && existing.metric.equals(metric) == false) {
            throw new IllegalStateException(
                "metric [" + id + "] is already registered as [" + existing.metric + "], not as [" + metric + "]"
            );
        }
        return existing;
    }

    /** Fails before registration when {@code identities} more identities exceed {@link #MAX_METRICS}. */
    private void ensureRoom(Metric.Id id, int identities) {
        if (registered.size() + identities > MAX_METRICS) {
            throw new IllegalStateException("a collector accepts at most [" + MAX_METRICS + "] metrics, cannot add [" + id + "]");
        }
    }

    /** Whether no metric is registered. This is different from all values being zero. */
    boolean isEmpty() {
        return registered.isEmpty();
    }

    /**
     * The current value of every registered metric. A timer reports an estimate when calls went unmeasured. After an
     * invalid CPU reading in this collector or a parent or detail of it, no timer reports CPU time. The times of a timer
     * never decrease from one snapshot to the next.
     */
    public Snapshot snapshot() {
        final List<Measurement> measurements = new ArrayList<>(registered.size());
        for (Registered entry : registered.values()) {
            if (entry.counter != null) {
                measurements.add(new CounterMeasurement((CounterMetric) entry.metric, entry.counter.value()));
            } else if (entry.cpuView == false) {
                final Timer timer = entry.timer;
                final OptionalLong cpu = timer.measuresCpu() && cpuState.failed == false
                    ? OptionalLong.of(timer.cpuEstimate())
                    : OptionalLong.empty();
                measurements.add(
                    new TimerMeasurement(
                        (TimerMetric) entry.metric,
                        timer.estimate(),
                        cpu,
                        timer.measuredCalls(),
                        timer.calls(),
                        timer.measuredCalls() < timer.calls()
                    )
                );
            }
        }
        return Snapshot.of(measurements);
    }

    /**
     * The estimated bytes of an object with {@code references} references, {@code longs} longs and {@code booleans}
     * booleans, on a heap with compressed references: a 12-byte header, 4 bytes for each reference, rounded up to 8.
     * Without compressed references, the estimate is too low.
     */
    static long shallowBytes(int references, int longs, int booleans) {
        final long bytes = 12L + 4L * references + 8L * longs + booleans;
        return (bytes + 7) & ~7L;
    }

    /** A metric and its counter or timer. A CPU view is the entry for the CPU time companion of a timer. */
    private record Registered(Metric metric, @Nullable Counter counter, @Nullable Timer timer, boolean cpuView) {}
}
