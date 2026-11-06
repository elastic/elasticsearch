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

import java.math.BigInteger;
import java.util.function.LongSupplier;

/**
 * Times calls of one {@link TimerMetric} in one collector. A call is one {@link #start()} and {@link #stop()} pair.
 * Put {@link #stop()} in a {@code finally} block, so that a call that throws is also timed:
 *
 * <pre>
 * timer.start();
 * try {
 *     // the call
 * } finally {
 *     timer.stop();
 * }
 * </pre>
 *
 * <p>A timer counts every call, including a call that throws. Its {@link TimingPolicy} decides which calls are
 * measured calls, whose elapsed time is read from the clock. {@link TimingPolicy#ADAPTIVE} measures the first calls and
 * then a decreasing share, and estimates the time of the unmeasured calls. {@link TimingPolicy#EXACT} is the schedule
 * that measures every call. It uses the same accounting as {@code ADAPTIVE}, but each measured call stands only for
 * itself, so the estimate equals the sum of the measured times.
 *
 * <p>Elapsed time is wall-clock time. It can include page faults, waits for a cache or for remote storage, and the time of
 * nested calls. When the definition has a CPU time companion and the collector measures CPU time, the timer also reads
 * the thread CPU time of each measured call. An invalid CPU reading removes the CPU time from the collector and its
 * details for the rest of their life.
 *
 * <p>A timer belongs to a metric of a collector. Every reader of one collector that asks for the same
 * {@link TimerMetric} gets the same timer. Thus these rules apply to all readers of the collector, and to the parent
 * collector and all its details:
 * <ul>
 *   <li>A call of a metric must not start while another call of the same metric is running. Nested calls of different
 *   metrics are allowed.</li>
 *   <li>A call starts and stops on the same thread, because CPU time is the time of the current thread.</li>
 *   <li>The timer has one writer at a time. It moves to another thread only between calls, through a handoff that
 *   orders the writes.</li>
 * </ul>
 * Assertions check these rules. With assertions disabled, overlapping calls corrupt the time of the calls involved.
 *
 * <p>The timer of a detail decides from its own call count which calls to measure. The timer of its parent decides
 * from the parent's call count. When one of them measures a call, the clocks are read once and the reading goes to each
 * timer that measures the call. The parent thus ends in the same state as without the detail.
 *
 * <p>The class does not implement {@link AutoCloseable}: one timer serves many calls, and an object per call would
 * allocate on the read path.
 */
public final class Timer {

    /**
     * The timer of a collector that does not measure time, and of a reader with no recorder. It counts nothing, reads
     * no clock and keeps no state, so one instance serves all threads.
     */
    public static final Timer INERT = new Timer();

    /** Null for {@link #INERT} only. */
    @Nullable
    private final LongSupplier clock;
    private final TimingPolicy policy;
    /** The parent collector's timer for the same metric, or null for a timer that is not a detail's. */
    @Nullable
    private final Timer parent;
    private long calls;
    private long measuredCalls;
    /** The value of {@link #calls} at the last measured call. */
    private long measuredUpTo;
    /** The elapsed time of the calls up to {@link #measuredUpTo}, each measured time multiplied by the calls it stands for. */
    private long weighted;
    private long published;
    private long startedAt;
    private boolean active;
    private boolean measuring;
    /** Whether the parent decided to measure the call in flight. Only a detail's timer uses it. */
    private boolean parentMeasuring;

    /** Null when the definition has no CPU time companion or the collector does not measure CPU time. */
    @Nullable
    private final LongSupplier cpuClock;
    @Nullable
    private final CpuState cpuState;
    /** The CPU time of the measured calls, under the same cursor and weights as {@link #weighted}. */
    private long cpuWeighted;
    private long cpuPublished;
    private long cpuStartedAt;
    private boolean cpuMeasuring;

    Timer(LongSupplier clock, TimingPolicy policy, @Nullable Timer parent, @Nullable LongSupplier cpuClock, @Nullable CpuState cpuState) {
        assert (cpuClock == null) == (cpuState == null) : "a CPU clock comes with the state that latches its failure";
        assert parent == null || parent.cpuClock == cpuClock : "a detail's timer measures CPU time exactly when its parent's does";
        this.clock = clock;
        this.policy = policy;
        this.parent = parent;
        this.cpuClock = cpuClock;
        this.cpuState = cpuState;
    }

    private Timer() {
        this.clock = null;
        this.policy = TimingPolicy.EXACT;
        this.parent = null;
        this.cpuClock = null;
        this.cpuState = null;
    }

    /** Whether this timer measures the CPU time of the calls it measures. */
    boolean measuresCpu() {
        return cpuClock != null;
    }

    long calls() {
        return calls;
    }

    long measuredCalls() {
        return measuredCalls;
    }

    /** Starts a call. It counts the call and reads the clocks when the call is measured. */
    public void start() {
        if (clock == null) {
            return;
        }
        if (parent != null) {
            startLinked();
            return;
        }
        assert active == false : "start was called again before the previous call stopped";
        active = true;
        measuring = wantsToMeasure();
        if (measuring) {
            beginInterval();
        }
        calls = increment(calls);
    }

    /** Stops the call started by {@link #start()} and records its time when the call is measured. */
    public void stop() {
        if (clock == null) {
            return;
        }
        if (parent != null) {
            stopLinked();
            return;
        }
        assert active : "stop was called with no call started";
        active = false;
        if (measuring) {
            endInterval();
        }
    }

    private void startLinked() {
        assert parent.active == false : "start was called again before the previous call stopped";
        assert active == false : "start was called again before the previous call stopped";
        parent.active = true;
        active = true;
        parentMeasuring = parent.wantsToMeasure();
        measuring = wantsToMeasure();
        if (parentMeasuring || measuring) {
            beginInterval();
        }
        parent.calls = increment(parent.calls);
        calls = increment(calls);
    }

    private void stopLinked() {
        assert parent.active : "stop was called with no call started";
        assert active : "stop was called with no call started";
        parent.active = false;
        active = false;
        if (parentMeasuring || measuring) {
            endInterval();
        }
    }

    /**
     * Reads the clocks that open the call in flight. The elapsed clock is read first here and last in
     * {@link #endInterval()}, so the CPU window sits strictly inside the elapsed window and the CPU clock's own read
     * cost lands in the elapsed value. The CPU clock is read only while no CPU reading of the collector has failed.
     */
    private void beginInterval() {
        startedAt = clock.getAsLong();
        cpuMeasuring = cpuClock != null && cpuState.failed == false;
        if (cpuMeasuring) {
            cpuStartedAt = cpuClock.getAsLong();
        }
    }

    /**
     * Reads the clocks that close the call in flight and records the one reading into each timer that measures the
     * call: this timer and, for a detail, its parent. Only {@link #startLinked()} sets {@link #parentMeasuring}. A
     * measured call is at least 1 nanosecond.
     */
    private void endInterval() {
        final long cpuElapsed = cpuMeasuring ? cpuElapsed() : -1;
        final long elapsed = Math.max(clock.getAsLong() - startedAt, 1L);
        if (parentMeasuring) {
            parent.record(elapsed, cpuElapsed);
        }
        if (measuring) {
            record(elapsed, cpuElapsed);
        }
    }

    /**
     * Whether the policy measures the next call, decided from the call count of this timer. This decision is the only
     * difference between the policies.
     */
    private boolean wantsToMeasure() {
        return policy == TimingPolicy.EXACT || (calls - measuredUpTo) >= Math.min(measuredUpTo >>> 8, 1024);
    }

    /** The CPU time of the call in flight, read now, or -1 after an invalid reading, which fails the CPU state. */
    private long cpuElapsed() {
        final long cpuStoppedAt = cpuClock.getAsLong();
        if (cpuStartedAt < 0 || cpuStoppedAt < cpuStartedAt) {
            // Fail closed: an invalid reading means the clock cannot be trusted, so no CPU value is
            // recorded again and the collector omits the CPU metrics from now on.
            cpuState.failed = true;
            return -1;
        }
        return cpuStoppedAt - cpuStartedAt;
    }

    /** Accounts a measured call: it stands for the calls since the previous measured one. */
    private void record(long elapsed, long cpuElapsed) {
        final long stoodFor = calls - measuredUpTo;
        weighted = saturatedAdd(weighted, saturatedMultiply(stoodFor, elapsed));
        if (cpuElapsed >= 0) {
            // Zero is a real sample: a short call can accumulate no CPU time.
            cpuWeighted = saturatedAdd(cpuWeighted, saturatedMultiply(stoodFor, cpuElapsed));
        }
        measuredUpTo = calls;
        measuredCalls = increment(measuredCalls);
    }

    /**
     * The estimated elapsed time: the weighted measured time, plus the calls after the last measured call at the mean
     * of the calls up to it. The returned value never decreases from one call of this method to the next.
     */
    long estimate() {
        published = Math.max(published, estimate(calls, measuredUpTo, weighted));
        return published;
    }

    /** As {@link #estimate()}, for the CPU side, over the same cursor. Meaningless once the CPU state failed. */
    long cpuEstimate() {
        cpuPublished = Math.max(cpuPublished, estimate(calls, measuredUpTo, cpuWeighted));
        return cpuPublished;
    }

    /**
     * {@code weighted + (calls - measuredUpTo) * weighted / measuredUpTo}, with the product taken before the division
     * so the remainder is kept. Only a result that does not fit in a long is saturated: the product can overflow
     * while the quotient fits.
     */
    static long estimate(long calls, long measuredUpTo, long weighted) {
        if (measuredUpTo == 0) {
            return 0;
        }
        final long unmeasured = calls - measuredUpTo;
        if (unmeasured <= 0) {
            return weighted;
        }
        long tail;
        try {
            tail = Math.multiplyExact(unmeasured, weighted) / measuredUpTo;
        } catch (ArithmeticException e) {
            final BigInteger exact = BigInteger.valueOf(unmeasured)
                .multiply(BigInteger.valueOf(weighted))
                .divide(BigInteger.valueOf(measuredUpTo));
            tail = exact.bitLength() > 63 ? Long.MAX_VALUE : exact.longValue();
        }
        return saturatedAdd(weighted, tail);
    }

    /** One more call, held at {@code Long.MAX_VALUE} like every count of this package. */
    private static long increment(long count) {
        return count == Long.MAX_VALUE ? count : count + 1;
    }

    private static long saturatedAdd(long a, long b) {
        final long sum = a + b;
        return sum < 0 ? Long.MAX_VALUE : sum;
    }

    private static long saturatedMultiply(long a, long b) {
        try {
            return Math.multiplyExact(a, b);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
