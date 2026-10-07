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
 * pauses the enclosing measurement until it returns, so no CPU time is counted twice. {@link #inheritMeteredCpu} uses
 * the stack to find the tracker that is metering the calling thread.
 * <p>
 * {@link #meteredCpu(ActionListener)} carries metering across async boundaries: the completion is measured on
 * whichever thread completes the listener. {@link #finish()} settles the calling thread's open measurement and
 * freezes the total, so nothing that runs after it is counted, even inside a measurement that is still open.
 */
public final class PlanningCpuTracker {

    /** Innermost open measurement on this thread, whichever tracker owns it. */
    private static final ThreadLocal<Measurement> CURRENT = new ThreadLocal<>();

    private static final long SETTLED = -1;
    private static final long PAUSED = -2;

    private final LongSupplier cpuClock;
    private final LongAdder cpuNanos = new LongAdder();
    private volatile boolean finished;

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

    private static final class Measurement {
        final PlanningCpuTracker owner;
        final Measurement outer;
        /** Thread CPU at the latest (re)start, or {@code SETTLED} / {@code PAUSED}. Only touched by the owning thread. */
        long startCpuNanos;

        Measurement(PlanningCpuTracker owner, Measurement outer, long startCpuNanos) {
            this.owner = owner;
            this.outer = outer;
            this.startCpuNanos = startCpuNanos;
        }

        void pause(long nowCpuNanos) {
            if (startCpuNanos >= 0) {
                owner.add(nowCpuNanos - startCpuNanos);
                startCpuNanos = PAUSED;
            }
        }

        void resume(long nowCpuNanos) {
            if (startCpuNanos == PAUSED) {
                startCpuNanos = nowCpuNanos;
            }
        }

        void settle(long nowCpuNanos) {
            if (startCpuNanos >= 0) {
                owner.add(nowCpuNanos - startCpuNanos);
            }
            startCpuNanos = SETTLED;
        }
    }

    /**
     * Runs {@code work} on the current thread and measures the CPU time it uses. A nested call on the same tracker is
     * counted by the enclosing measurement. A measurement of another tracker that is open on this thread is paused
     * while {@code work} runs and resumed afterwards.
     */
    public <T, E extends Exception> T meteredCpu(CheckedSupplier<T, E> work) throws E {
        Measurement outer = CURRENT.get();
        if (outer != null && outer.owner == this) {
            return work.get();
        }
        long startCpuNanos = cpuClock.getAsLong();
        if (startCpuNanos < 0) {
            return work.get();
        }
        if (outer != null) {
            outer.pause(startCpuNanos);
        }
        Measurement measurement = new Measurement(this, outer, startCpuNanos);
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
     */
    public <T> ActionListener<T> meteredCpu(ActionListener<T> listener) {
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
        Measurement measurement = CURRENT.get();
        return measurement == null ? listener : measurement.owner.meteredCpu(listener);
    }

    /**
     * Commits this thread's open measurement so far and restarts it. Call it right before signalling another thread
     * that may go on to finish planning (releasing a fan-out permit), so the work done so far survives a
     * {@link #finish()} on that other thread. Only the unwind after the signal can still be dropped.
     */
    public void checkpoint() {
        Measurement measurement = CURRENT.get();
        if (measurement == null || measurement.owner != this || measurement.startCpuNanos < 0) {
            return;
        }
        long nowCpuNanos = cpuClock.getAsLong();
        add(nowCpuNanos - measurement.startCpuNanos);
        measurement.startCpuNanos = nowCpuNanos;
    }

    /**
     * Planning end. Settles this thread's open measurement, freezes the total and returns it. Later commits are
     * dropped, so execution that continues on this thread inside the same measurement adds nothing. Idempotent.
     */
    public long finish() {
        Measurement measurement = CURRENT.get();
        if (measurement != null && measurement.owner == this) {
            measurement.settle(cpuClock.getAsLong());
        }
        finished = true;
        return cpuNanos.sum();
    }

    /** Live total. */
    public long cpuNanos() {
        return cpuNanos.sum();
    }

    /** For assertions. True when a measurement of this tracker is open on this thread, or when thread CPU time is unsupported. */
    public boolean isMeteringCurrentThread() {
        Measurement measurement = CURRENT.get();
        return (measurement != null && measurement.owner == this) || cpuClock.getAsLong() < 0;
    }

    private void add(long deltaNanos) {
        if (finished == false && deltaNanos > 0) {
            cpuNanos.add(deltaNanos);
        }
    }
}
