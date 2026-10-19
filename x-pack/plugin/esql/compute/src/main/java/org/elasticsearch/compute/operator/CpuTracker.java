/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Sums the thread CPU time one unit of work, such as a driver or a query's planning, consumes on whichever threads it
 * runs.
 * <p>
 * The total is the sum of <em>measurements</em>. A measurement is the CPU time one thread uses while it is open. Only
 * that time is counted, so waits between measurements are not. When the JVM cannot measure thread CPU time, nothing is
 * counted and the total stays 0.
 * <p>
 * Each thread keeps one stack of open measurements, shared by all trackers, and each measurement records the tracker
 * that owns it. Only the innermost one counts: opening a measurement pauses the enclosing one until it closes, so no
 * CPU time is counted by two trackers. Opening one when the innermost already belongs to the same tracker measures
 * nothing, because that one already counts the time. A tracker can still have more than one measurement on a stack,
 * such as when a measurement of another tracker sits between them. The outer ones stay paused.
 * <p>
 * A measurement adds its CPU time to the total when it closes or is {@link #checkpoint() checkpointed}. Code that reads
 * the total on another thread, such as a final report, misses what an open measurement has used since it last added,
 * so call {@link #checkpoint()} right before signalling a thread that may read the final total.
 */
public class CpuTracker {

    /** Innermost open measurement on this thread, whichever tracker owns it. */
    private static final ThreadLocal<Measurement> CURRENT = new ThreadLocal<>();

    private static final Releasable NOOP = () -> {};

    /** Lifecycle of a {@link Measurement}. Only a running one counts CPU time. */
    private enum State {
        /** The innermost open measurement on its thread. */
        RUNNING,
        /** A measurement of another tracker is open inside it and counts the time instead. */
        PAUSED
    }

    private final LongSupplier cpuClock;
    private final LongAdder cpuNanos = new LongAdder();

    public CpuTracker() {
        this(ThreadCpuTimer::currentNanos);
    }

    /**
     * {@code cpuClock} must return the calling thread's CPU time in nanoseconds, or a negative value when thread CPU
     * time is unsupported. A parameter so tests can control it.
     */
    protected CpuTracker(LongSupplier cpuClock) {
        this.cpuClock = cpuClock;
    }

    /** One open measurement of the enclosing tracker, its owner. Only touched by the thread that opened it. */
    private final class Measurement implements Releasable {
        /** The measurement this one is nested in, restored when this one closes. */
        @Nullable
        private final Measurement outer;
        /** Thread CPU at the latest (re)start. A negative reading stops counting. */
        private long startCpuNanos;
        private State state = State.RUNNING;

        Measurement(@Nullable Measurement outer, long startCpuNanos) {
            this.outer = outer;
            this.startCpuNanos = startCpuNanos;
        }

        CpuTracker owner() {
            return CpuTracker.this;
        }

        /** Adds the CPU time since the latest (re)start to the owner. Does nothing unless running. */
        void commit(long nowCpuNanos) {
            if (state == State.RUNNING && startCpuNanos >= 0) {
                add(nowCpuNanos - startCpuNanos);
            }
        }

        void pause(long nowCpuNanos) {
            commit(nowCpuNanos);
            state = State.PAUSED;
        }

        void resume(long nowCpuNanos) {
            if (state == State.PAUSED) {
                state = State.RUNNING;
                startCpuNanos = nowCpuNanos;
            }
        }

        @Override
        public void close() {
            assert CURRENT.get() == this : "measurements close on the thread that opened them, innermost first";
            long endCpuNanos = cpuClock.getAsLong();
            commit(endCpuNanos);
            if (outer == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(outer);
                outer.resume(endCpuNanos);
            }
        }
    }

    /**
     * Opens a measurement of the CPU time the current thread uses until the returned {@link Releasable} is closed.
     * Private so every measurement is opened and closed by {@link #meteredCpu(CheckedSupplier)} or
     * {@link #meteredCpu(CheckedRunnable)}, on one thread and innermost first.
     */
    private Releasable meteredCpu() {
        Measurement outer = CURRENT.get();
        if (outer != null && outer.owner() == this) {
            return NOOP;
        }
        long startCpuNanos = cpuClock.getAsLong();
        if (startCpuNanos < 0) {
            return NOOP;
        }
        if (outer != null) {
            outer.pause(startCpuNanos);
        }
        Measurement measurement = new Measurement(outer, startCpuNanos);
        CURRENT.set(measurement);
        return measurement;
    }

    /**
     * Runs {@code work} on the current thread and counts the CPU time it uses. Measures nothing when the innermost open
     * measurement belongs to this tracker. Otherwise pauses the innermost open measurement, if any, until {@code work}
     * returns.
     */
    public <T, E extends Exception> T meteredCpu(CheckedSupplier<T, E> work) throws E {
        try (Releasable ignored = meteredCpu()) {
            return work.get();
        }
    }

    /** {@link #meteredCpu(CheckedSupplier)} for work that returns nothing. */
    public <E extends Exception> void meteredCpu(CheckedRunnable<E> work) throws E {
        try (Releasable ignored = meteredCpu()) {
            work.run();
        }
    }

    /**
     * Runs the listener's {@code onResponse} and {@code onFailure} inside {@link #meteredCpu(CheckedRunnable)}. Apply it
     * to the listener handed to an async API, because that listener is the first thing the completing thread calls.
     * <p>
     * Wrapping also {@link #checkpoint() checkpoints} the calling thread's open measurement. The wrapper is built as the
     * argument of the async dispatch, so the work done so far is added before the completing thread can go on to read
     * the final total.
     */
    public <T> ActionListener<T> meteredCpu(ActionListener<T> listener) {
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
                return "meteredCpu[" + listener + "]";
            }
        };
    }

    /**
     * Adds the CPU time this thread's open measurement of this tracker has used so far and restarts it. Does nothing
     * when the innermost open measurement belongs to another tracker or none is open.
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

    /** The total so far. Does not include what open measurements have used since they last added. */
    public long cpuNanos() {
        return cpuNanos.sum();
    }

    /** For assertions. True when this tracker owns this thread's innermost open measurement, or thread CPU time is unsupported. */
    public boolean isMeteringCurrentThread() {
        Measurement measurement = CURRENT.get();
        return (measurement != null && measurement.owner() == this) || cpuClock.getAsLong() < 0;
    }

    /** The tracker that owns the calling thread's innermost open measurement, or {@code null} when none is open. */
    @Nullable
    protected static CpuTracker currentThreadTracker() {
        Measurement measurement = CURRENT.get();
        return measurement == null ? null : measurement.owner();
    }

    /** Adds a measured CPU time to the total. Subclasses may override it to stop counting, but must call it to count. */
    protected void add(long deltaNanos) {
        if (deltaNanos > 0) {
            cpuNanos.add(deltaNanos);
        }
    }
}
