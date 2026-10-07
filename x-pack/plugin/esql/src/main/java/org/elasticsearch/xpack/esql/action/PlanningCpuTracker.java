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
 * {@code planning_nanos} is wall time: it includes object-store listings and metadata reads, field caps, enrich
 * and inference round trips, and queue waits, and Serverless bills it as if it were CPU. This tracker is the CPU
 * counterpart: ThreadMXBean CPU time summed over <em>samples</em>, where a sample is one piece of planning work on
 * one thread. Waits fall between samples and are never counted.
 * <p>
 * Shaped after {@code ExternalReadCounters.meteredCpu}: same verb, same supplier and runnable shapes, same nesting
 * rule (a nested call on the same tracker is counted by the enclosing sample). It differs in three ways. One shared
 * thread-local stack with an owner per sample, so another query's work that runs inline on this thread pauses our
 * sample instead of being double counted, and {@link #inheritMeteredCpu} can find the query a thread is running.
 * A listener overload, because planning hops threads at every async boundary. And {@link #finish()}, because the
 * last planning continuation runs straight into execution on the same thread and its sample must be cut explicitly.
 * <p>
 * Known gaps, shared with {@code read_cpu_nanos}: CPU on SDK/Netty threads before our callback runs (for example
 * the Parquet footer-tail copy), transport (de)serialization, and framework code outside the metered continuations.
 */
public final class PlanningCpuTracker {

    /** Innermost open sample on this thread, whichever tracker owns it. */
    private static final ThreadLocal<Sample> CURRENT = new ThreadLocal<>();

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

    private static final class Sample {
        final PlanningCpuTracker owner;
        final Sample outer;
        /** Thread CPU at the latest (re)start, or {@code SETTLED} / {@code PAUSED}. Only touched by the owning thread. */
        long startCpuNanos;

        Sample(PlanningCpuTracker owner, Sample outer, long startCpuNanos) {
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
     * Runs {@code work} as a sample of this tracker on the current thread. A nested call on the same tracker is
     * counted by the enclosing sample. A sample of another tracker that is open on this thread is paused while
     * {@code work} runs and resumed afterwards.
     */
    public <T, E extends Exception> T meteredCpu(CheckedSupplier<T, E> work) throws E {
        Sample outer = CURRENT.get();
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
        Sample sample = new Sample(this, outer, startCpuNanos);
        CURRENT.set(sample);
        try {
            return work.get();
        } finally {
            long endCpuNanos = cpuClock.getAsLong();
            sample.settle(endCpuNanos);
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
        Sample sample = CURRENT.get();
        return sample == null ? listener : sample.owner.meteredCpu(listener);
    }

    /**
     * Commits this thread's open sample so far and restarts it. Call it right before signalling another thread
     * that may go on to finish planning (releasing a fan-out permit), so the work done so far survives a
     * {@link #finish()} on that other thread. Only the unwind after the signal can still be dropped.
     */
    public void checkpoint() {
        Sample sample = CURRENT.get();
        if (sample == null || sample.owner != this || sample.startCpuNanos < 0) {
            return;
        }
        long nowCpuNanos = cpuClock.getAsLong();
        add(nowCpuNanos - sample.startCpuNanos);
        sample.startCpuNanos = nowCpuNanos;
    }

    /**
     * Planning end. Settles this thread's open sample, freezes the total and returns it. Later commits are
     * dropped, so execution that continues on this thread inside the same sample adds nothing. Idempotent.
     */
    public long finish() {
        Sample sample = CURRENT.get();
        if (sample != null && sample.owner == this) {
            sample.settle(cpuClock.getAsLong());
        }
        finished = true;
        return cpuNanos.sum();
    }

    /** Live total. */
    public long cpuNanos() {
        return cpuNanos.sum();
    }

    /** For assertions. True when a sample of this tracker is open on this thread, or when thread CPU time is unsupported. */
    public boolean isMeteringCurrentThread() {
        Sample sample = CURRENT.get();
        return (sample != null && sample.owner == this) || cpuClock.getAsLong() < 0;
    }

    private void add(long deltaNanos) {
        if (finished == false && deltaNanos > 0) {
            cpuNanos.add(deltaNanos);
        }
    }
}
