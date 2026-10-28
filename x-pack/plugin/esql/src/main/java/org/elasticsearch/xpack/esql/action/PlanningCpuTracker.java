/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.operator.CpuTracker;
import org.elasticsearch.compute.operator.ThreadCpuTimer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Sums the thread CPU time one query's planning code consumes, on whichever threads it runs. See {@link CpuTracker}
 * for how measurements nest.
 * <p>
 * {@link #meteredCpu(ActionListener)} carries metering across async boundaries: the completion is measured on
 * whichever thread completes the listener. {@link #finish()} adds the calling thread's open measurement and freezes
 * the total, so nothing that runs after it is counted, even inside a measurement that is still open.
 * <p>
 * Code that holds the query's tracker calls it directly. Code that does not hold it inherits it from the calling thread with
 * {@link #inheritMeteredCpu} and {@link #checkpointCurrentThread}, which only works for hand-offs through listeners the
 * code wraps itself. An executor handed to code that hops threads on its own, such as a format reader completing a
 * storage read on an SDK thread before it submits the parse, must be metered by a tracker the executor holds: the
 * submitting thread may not be metered. {@link #UNMETERED} stands in when there is no tracker.
 */
public final class PlanningCpuTracker extends CpuTracker {

    /** {@link #finishedCpuNanos} sentinel. Never a total, which is never negative. */
    private static final long NOT_FINISHED = -1;

    /** A tracker that measures nothing. Stands in where no query's planning is being metered, so callers need no null checks. */
    public static final PlanningCpuTracker UNMETERED = new PlanningCpuTracker(() -> -1);

    /**
     * The total frozen by the first {@link #finish()}, or {@code NOT_FINISHED} before it. A commit that read
     * {@code NOT_FINISHED} just before {@link #finish()} can still land in the running sum afterwards, so later reads
     * report this value rather than the sum.
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
        super(cpuClock);
    }

    /** {@link CpuTracker#meteredCpu(ActionListener)}, except {@link #UNMETERED} returns {@code listener} unchanged. */
    @Override
    public <T> ActionListener<T> meteredCpu(ActionListener<T> listener) {
        if (this == UNMETERED) {
            return listener;
        }
        return super.meteredCpu(listener);
    }

    /**
     * {@link #meteredCpu(ActionListener)} for whichever planning tracker is metering the calling thread, or
     * {@code listener} unchanged when none is. It reads the thread-local on the calling thread, so it must be written
     * as the argument expression of the async dispatch it decorates, never stored and applied later.
     */
    public static <T> ActionListener<T> inheritMeteredCpu(ActionListener<T> listener) {
        return current().meteredCpu(listener);
    }

    /**
     * {@link #checkpoint()} for whichever planning tracker is metering the calling thread, or nothing when none is. For
     * code that signals another thread but has no tracker in hand, such as a listing fan-out that may end on another
     * thread.
     */
    public static void checkpointCurrentThread() {
        current().checkpoint();
    }

    /**
     * The planning tracker that owns the calling thread's innermost open measurement, or {@link #UNMETERED} when none
     * is open or the innermost one belongs to another kind of tracker.
     */
    private static PlanningCpuTracker current() {
        return currentThreadTracker() instanceof PlanningCpuTracker planning ? planning : UNMETERED;
    }

    /**
     * Planning end. Adds this thread's open measurement, freezes the total and returns it. Later measurements add
     * nothing, so execution that continues on this thread inside the same measurement is not counted. Idempotent: later
     * calls return the total the first call froze. {@link #UNMETERED} always returns 0 and is never frozen, because it
     * is shared by every caller that has no tracker.
     */
    public long finish() {
        if (this == UNMETERED) {
            return 0;
        }
        checkpoint();
        finishedCpuNanos.compareAndSet(NOT_FINISHED, super.cpuNanos());
        return finishedCpuNanos.get();
    }

    /** The total so far, or the frozen total once {@link #finish()} has run. */
    @Override
    public long cpuNanos() {
        long finishedNanos = finishedCpuNanos.get();
        return finishedNanos == NOT_FINISHED ? super.cpuNanos() : finishedNanos;
    }

    @Override
    protected void add(long deltaNanos) {
        // UNMETERED never gets here: its clock reports CPU time unsupported, so no measurement of it is ever opened.
        if (finishedCpuNanos.get() == NOT_FINISHED) {
            super.add(deltaNanos);
        }
    }
}
