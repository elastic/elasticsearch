/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.xpack.esql.datasources.spi.ThreadCpuTimer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Sums the thread CPU time one query's planning code consumes, on whichever threads it runs.
 * <p>
 * The total is the sum of <em>measurements</em>. A measurement is the CPU time one thread uses while it runs one piece
 * of work inside {@link #meteredCpu}. Only that work is measured, so waits between measurements are not counted. When
 * the JVM cannot measure thread CPU time, nothing is counted and the total stays 0.
 * <p>
 * Each thread keeps one stack of open measurements, shared by all trackers, and each measurement records the tracker
 * that owns it. A nested call on the same tracker is counted by the enclosing measurement. A call on another tracker
 * pauses the enclosing measurement until it returns, so no CPU time is counted twice. {@link #inheritMeteredCpu} and
 * {@link #checkpointCurrentThread} use the stack to find the tracker that is metering the calling thread.
 * <p>
 * {@link #meteredCpu(ActionListener)} carries metering across async boundaries: the completion is measured on
 * whichever thread completes the listener. {@link #finish()} settles the calling thread's open measurement and
 * freezes the total, so nothing that runs after it is counted, even inside a measurement that is still open.
 * <p>
 * Code that holds the query's tracker calls it directly. Code that does not hold it inherits it from the calling thread with
 * {@link #inheritMeteredCpu} and {@link #checkpointCurrentThread}, which only works for hand-offs through listeners the
 * code wraps itself. An executor handed to code that hops threads on its own, such as a format reader completing a
 * storage read on an SDK thread before it submits the parse, must be metered by a tracker the executor holds: the
 * submitting thread may not be metered. {@link #UNMETERED} stands in when there is no tracker.
 */
public final class PlanningCpuTracker {

    /** Innermost open measurement on this thread, whichever tracker owns it. */
    private static final ThreadLocal<Measurement> CURRENT = new ThreadLocal<>();

    /** {@link Measurement#startCpuNanos} sentinels. Negative, so they never collide with a thread CPU reading. */
    private static final long SETTLED = -1;
    private static final long PAUSED = -2;
    /** {@link #finishedCpuNanos} sentinel. Distinct from the measurement sentinels, so they cannot be confused. */
    private static final long NOT_FINISHED = -3;

    /** A tracker that measures nothing. Stands in where no query's planning is being metered, so callers need no null checks. */
    public static final PlanningCpuTracker UNMETERED = new PlanningCpuTracker(() -> -1);

    private final LongSupplier cpuClock;
    private final LongAdder cpuNanos = new LongAdder();
    /**
     * The total frozen by the first {@link #finish()}, or {@code NOT_FINISHED} before it. A commit that read
     * {@code NOT_FINISHED} just before {@link #finish()} can still land in {@code cpuNanos} afterwards, so later reads
     * report this value rather than the adder.
     */
    private final AtomicLong finishedCpuNanos = new AtomicLong(NOT_FINISHED);

    public PlanningCpuTracker() {
        this(ThreadCpuTimer::currentNanos);
    }

    /**
     * Visible for testing. {@code cpuClock} must return the calling thread's CPU time in nanoseconds, or a
     * negative value when thread CPU time is unsupported.
     */
    PlanningCpuTracker(LongSupplier cpuClock) {
        this.cpuClock = cpuClock;
    }

    /** One measurement of the enclosing tracker, its owner. */
    private final class Measurement {
        /** Thread CPU at the latest (re)start, or {@code SETTLED} / {@code PAUSED}. Only touched by the owning thread. */
        long startCpuNanos;

        Measurement(long startCpuNanos) {
            this.startCpuNanos = startCpuNanos;
        }

        PlanningCpuTracker owner() {
            return PlanningCpuTracker.this;
        }

        /** Adds the CPU time since the latest (re)start to the owner. Does nothing while paused or settled. */
        void commit(long nowCpuNanos) {
            if (startCpuNanos >= 0) {
                add(nowCpuNanos - startCpuNanos);
            }
        }

        void pause(long nowCpuNanos) {
            if (startCpuNanos >= 0) {
                commit(nowCpuNanos);
                startCpuNanos = PAUSED;
            }
        }

        void resume(long nowCpuNanos) {
            if (startCpuNanos == PAUSED) {
                startCpuNanos = nowCpuNanos;
            }
        }

        void settle(long nowCpuNanos) {
            commit(nowCpuNanos);
            startCpuNanos = SETTLED;
        }
    }

    /**
     * Runs {@code work} on the current thread and measures the CPU time it uses. A nested call on the same tracker is
     * counted by the enclosing measurement. A measurement of another tracker that is open on this thread is paused
     * while {@code work} runs and resumed afterwards.
     */
    public <T, E extends Exception> T meteredCpu(CheckedSupplier<T, E> work) throws E {
        if (this == UNMETERED) {
            return work.get();
        }
        Measurement outer = CURRENT.get();
        if (outer != null && outer.owner() == this) {
            return work.get();
        }
        long startCpuNanos = cpuClock.getAsLong();
        if (startCpuNanos < 0) {
            return work.get();
        }
        if (outer != null) {
            outer.pause(startCpuNanos);
        }
        Measurement measurement = new Measurement(startCpuNanos);
        CURRENT.set(measurement);
        try {
            return work.get();
        } finally {
            long endCpuNanos = cpuClock.getAsLong();
            measurement.settle(endCpuNanos);
            if (outer == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(outer);
                outer.resume(endCpuNanos);
            }
        }
    }

    /** {@link #meteredCpu(CheckedSupplier)} for work that returns nothing. */
    public <E extends Exception> void meteredCpu(CheckedRunnable<E> work) throws E {
        meteredCpu(() -> {
            work.run();
            return null;
        });
    }

    /**
     * Runs the listener's {@code onResponse} and {@code onFailure} inside {@link #meteredCpu}. Apply it to the
     * listener handed to an async API, because that listener is the first thing the completing thread calls.
     * <p>
     * Wrapping also {@link #checkpoint() checkpoints} the calling thread's open measurement. The wrapper is built as the
     * argument of the async dispatch, so the work done so far is committed before the completing thread can go on to
     * call {@link #finish()}.
     */
    public <T> ActionListener<T> meteredCpu(ActionListener<T> listener) {
        if (this == UNMETERED) {
            return listener;
        }
        checkpoint();
        return new ActionListener<>() {
            @Override
            public void onResponse(T response) {
                meteredCpu(() -> listener.onResponse(response));
            }

            @Override
            public void onFailure(Exception e) {
                meteredCpu(() -> listener.onFailure(e));
            }

            @Override
            public String toString() {
                return "planningCpu[" + listener + "]";
            }
        };
    }

    /**
     * {@link #meteredCpu(ActionListener)} for whichever tracker is metering the calling thread, or {@code listener}
     * unchanged when none is. It reads the thread-local on the calling thread, so it must be written as the
     * argument expression of the async dispatch it decorates, never stored and applied later.
     */
    public static <T> ActionListener<T> inheritMeteredCpu(ActionListener<T> listener) {
        return current().meteredCpu(listener);
    }

    /**
     * Commits this thread's open measurement so far and restarts it. Call it right before signalling another thread
     * that may go on to finish planning (releasing a fan-out permit), so the work done so far survives a
     * {@link #finish()} on that other thread. Only the unwind after the signal can still be dropped.
     * {@link #meteredCpu(ActionListener)} calls it for every async dispatch it wraps.
     */
    public void checkpoint() {
        Measurement measurement = CURRENT.get();
        if (measurement == null || measurement.owner() != this || measurement.startCpuNanos < 0) {
            return;
        }
        long nowCpuNanos = cpuClock.getAsLong();
        measurement.commit(nowCpuNanos);
        measurement.startCpuNanos = nowCpuNanos;
    }

    /**
     * {@link #checkpoint()} for whichever tracker is metering the calling thread, or nothing when none is. For code
     * that signals another thread but has no tracker in hand, such as a listing fan-out that may end on another thread.
     */
    public static void checkpointCurrentThread() {
        current().checkpoint();
    }

    /** The tracker that owns the calling thread's innermost open measurement, or {@link #UNMETERED} when none is open. */
    private static PlanningCpuTracker current() {
        Measurement measurement = CURRENT.get();
        return measurement == null ? UNMETERED : measurement.owner();
    }

    /**
     * Planning end. Settles this thread's open measurement, freezes the total and returns it. Later commits are
     * dropped, so execution that continues on this thread inside the same measurement adds nothing. Idempotent: later
     * calls return the total the first call froze. {@link #UNMETERED} always returns 0 and is never frozen, because it
     * is shared by every caller that has no tracker.
     */
    public long finish() {
        if (this == UNMETERED) {
            return 0;
        }
        Measurement measurement = CURRENT.get();
        if (measurement != null && measurement.owner() == this) {
            measurement.settle(cpuClock.getAsLong());
        }
        finishedCpuNanos.compareAndSet(NOT_FINISHED, cpuNanos.sum());
        return finishedCpuNanos.get();
    }

    /** The total so far, or the frozen total once {@link #finish()} has run. */
    public long cpuNanos() {
        long finishedNanos = finishedCpuNanos.get();
        return finishedNanos == NOT_FINISHED ? cpuNanos.sum() : finishedNanos;
    }

    /** For assertions. True when a measurement of this tracker is open on this thread, or when thread CPU time is unsupported. */
    public boolean isMeteringCurrentThread() {
        Measurement measurement = CURRENT.get();
        return (measurement != null && measurement.owner() == this) || cpuClock.getAsLong() < 0;
    }

    private void add(long deltaNanos) {
        // UNMETERED never gets here: its clock reports CPU time unsupported, so no measurement of it is ever opened.
        if (deltaNanos > 0 && finishedCpuNanos.get() == NOT_FINISHED) {
            cpuNanos.add(deltaNanos);
        }
    }
}
