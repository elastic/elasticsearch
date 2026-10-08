/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.AdmissionGate;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

/**
 * Node-level admission controller that caps how many {@link StreamingParallelParsingCoordinator} segmentators
 * may occupy the shared {@code esql_external_io} pool at once. A pinned parser pool is not a deadlock:
 * the segmentator inlines the FIFO head. This gate still keeps spare pool threads so parser tasks
 * remain a second progress path, not the only one.
 * <p>
 * <strong>The hazard it closes.</strong> Each open stream-only compressed read runs a single long-lived
 * segmentator task on {@code esql_external_io}; that task blocks on {@code dispatchPermits.acquire},
 * {@code bufferPool.take}, and the upstream decompress {@code InputStream.read} when no queued chunk
 * can be inlined. A full chunk queue no longer parks the segmentator: it parses the FIFO head on
 * its own thread. Parser tasks still share the same pool, so a full set of pinned segmentators can
 * starve those tasks — inline parse on the segmentator is the liveness path (T2). The gate still
 * keeps at least one pool thread free so parser tasks remain a second progress path
 * (elastic/esql-planning #1093, structural-fix item 4), independent of the drain-side fix in #153074.
 * <p>
 * <strong>Why the gate must precede submission.</strong> A semaphore acquired <em>inside</em> the segmentator
 * task would not help: a segmentator blocked on {@code acquire()} still holds its pool thread. This controller
 * therefore gates <em>before</em> handing the task to the executor. When the concurrency budget is exhausted the
 * segmentator is queued here (holding no pool thread) and dispatched only once a running segmentator completes
 * and frees its slot. Because at most {@link #maxConcurrentSegmentators} segmentators are ever handed to the pool,
 * at least {@code poolSize - maxConcurrentSegmentators} threads always remain available to run parser tasks, which
 * are short-lived and self-terminating, so the read always makes progress.
 * <p>
 * A single instance is created once per node and held by {@link FileSourceFactory} (itself a per-node singleton),
 * so every coordinator submitting to the shared read pool shares one budget — the hazard spans operators and
 * queries, not a single read. The submitting executor is supplied per {@link #submit} call rather than stored,
 * so the controller holds no reference to the pool. Use {@link #unbounded()} on test/benchmark paths that run on
 * an isolated, generously-sized pool where segmentator saturation cannot arise.
 * <p>
 * Pending work is cancellable: {@link Handle#cancel()} removes a still-queued segmentator so a consumer that
 * closes the iterator before the work is admitted does not wait for a slot. Cancel of already-dispatched work
 * is a no-op; that iterator's started {@code close()} path interrupts the running segmentator.
 */
final class StreamingSegmentatorAdmission implements AdmissionGate {

    private final int maxConcurrentSegmentators;
    private final AdmissionTracker tracker;

    /** Segmentators currently handed to the pool (running or queued in the pool's own work queue). Guarded by {@code this}. */
    private int running = 0;
    /** Segmentators admitted here but not yet handed to the pool because the budget was full. Guarded by {@code this}. */
    private final ArrayDeque<Deferred> pending = new ArrayDeque<>();

    /**
     * Ticket for one {@link #submit} call. {@link #cancel()} removes the work from {@link #pending} if it has
     * not yet been handed to the executor; already-dispatched work is left alone.
     */
    interface Handle {
        /**
         * Attempts to drop this submission before it occupies a pool thread.
         *
         * @return {@code true} if the work was still pending and will never run (caller must account for the
         *         never-started segmentator itself — this controller does not increment or decrement
         *         {@code running} for work that was never dispatched); {@code false} if the work was already
         *         handed to the executor, in which case this is a no-op
         */
        boolean cancel();
    }

    private static final Handle ALREADY_DISPATCHED = () -> false;

    /**
     * Identity-keyed pending item so {@link Handle#cancel()} can remove exactly this submission. Not a record:
     * structural equality would let two submissions with equal components cancel each other.
     */
    private static final class Deferred {
        private final Runnable segmentator;
        private final Executor executor;
        private final Consumer<RejectedExecutionException> onReject;
        private AdmissionTracker.Wait wait = AdmissionTracker.NOOP_WAIT;

        private Deferred(Runnable segmentator, Executor executor, Consumer<RejectedExecutionException> onReject) {
            this.segmentator = segmentator;
            this.executor = executor;
            this.onReject = onReject;
        }
    }

    private final class PendingHandle implements Handle {
        private final Deferred deferred;

        private PendingHandle(Deferred deferred) {
            this.deferred = deferred;
        }

        @Override
        public boolean cancel() {
            synchronized (StreamingSegmentatorAdmission.this) {
                boolean removed = pending.remove(deferred);
                if (removed) {
                    deferred.wait.finished();
                }
                return removed;
            }
        }
    }

    /**
     * An effectively-unbounded controller ({@link Integer#MAX_VALUE} budget) that dispatches every segmentator
     * immediately. Test/benchmark-only: use on isolated, generously-sized pools where the saturation hazard cannot
     * arise. Production always constructs a controller with a real, pool-derived cap.
     */
    static StreamingSegmentatorAdmission unbounded() {
        return new StreamingSegmentatorAdmission(Integer.MAX_VALUE);
    }

    StreamingSegmentatorAdmission(int maxConcurrentSegmentators) {
        this(maxConcurrentSegmentators, AdmissionTracker.NOOP);
    }

    StreamingSegmentatorAdmission(int maxConcurrentSegmentators, AdmissionTracker tracker) {
        this.maxConcurrentSegmentators = Math.max(1, maxConcurrentSegmentators);
        this.tracker = tracker == null ? AdmissionTracker.NOOP : tracker;
        this.tracker.register(this);
    }

    /**
     * Admits a segmentator: runs it on {@code executor} immediately if the concurrency budget allows, otherwise
     * queues it until a running segmentator completes. {@code onReject} runs (on some pool thread, possibly later)
     * if the executor refuses the task — the coordinator uses it to record the failure and wake its consumer,
     * exactly as the pre-admission direct-{@code execute} path did on a {@link RejectedExecutionException}. All
     * coordinators sharing this controller submit to the same node-level pool, so the executor is stable across
     * calls; it is passed per call so the controller need not hold a reference to it.
     * <p>
     * The executor hand-off, {@code onReject}, and any listener run <em>after</em> this monitor is released:
     * a callback that re-enters {@link #submit} or {@link Handle#cancel()} must not observe the lock held.
     */
    Handle submit(Runnable segmentator, Executor executor, Consumer<RejectedExecutionException> onReject) {
        Deferred toDispatch;
        Deferred queued;
        synchronized (this) {
            if (running < maxConcurrentSegmentators) {
                running++;
                toDispatch = new Deferred(segmentator, executor, onReject);
                queued = null;
            } else {
                queued = new Deferred(segmentator, executor, onReject);
                queued.wait = tracker.waitStarted(AdmissionTracker.GATE_SEGMENTATORS, Thread.currentThread().getName());
                pending.add(queued);
                toDispatch = null;
            }
        }
        if (toDispatch != null) {
            dispatch(toDispatch);
            return ALREADY_DISPATCHED;
        }
        return new PendingHandle(queued);
    }

    /**
     * Hands {@code d} to its executor, wrapping it so the slot is released and the next pending segmentator promoted
     * when it completes. On a {@link RejectedExecutionException} the reserved slot is freed and any promoted-then-
     * rejected successor is handled in the same loop, so a cascade of rejections (e.g. pool shutdown) unwinds
     * iteratively rather than recursively. {@code executor.execute} and {@code onReject} run outside the monitor.
     */
    private void dispatch(Deferred d) {
        while (d != null) {
            try {
                Deferred toRun = d;
                toRun.executor.execute(() -> {
                    try {
                        toRun.segmentator.run();
                    } finally {
                        dispatch(releaseAndPromote());
                    }
                });
                return;
            } catch (RejectedExecutionException e) {
                d.onReject.accept(e);
                d = releaseAndPromote();
            }
        }
    }

    /** Frees the slot held by a just-finished (or rejected) segmentator and returns the next pending one to dispatch, if any. */
    private synchronized Deferred releaseAndPromote() {
        running--;
        Deferred next = pending.poll();
        if (next != null) {
            running++;
            next.wait.granted();
        }
        return next;
    }

    int maxConcurrentSegmentators() {
        return maxConcurrentSegmentators;
    }

    /** Test-only: segmentators currently handed to the pool (running or queued in the pool). */
    synchronized int running() {
        return running;
    }

    /** Test-only: segmentators admitted but not yet handed to the pool because the budget was full. */
    synchronized int pending() {
        return pending.size();
    }

    @Override
    public String name() {
        return AdmissionTracker.GATE_SEGMENTATORS;
    }

    @Override
    public int holders() {
        synchronized (this) {
            return running;
        }
    }
}
