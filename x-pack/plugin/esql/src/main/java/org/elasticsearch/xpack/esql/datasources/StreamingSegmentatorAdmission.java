/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

/**
 * Node-level admission controller that caps how many {@link StreamingParallelParsingCoordinator} segmentators
 * may occupy the shared {@code esql_external_io} pool at once, guaranteeing that pool threads always remain free
 * to run the one-shot parser tasks a segmentator depends on.
 * <p>
 * <strong>The hazard it closes.</strong> Each open stream-only compressed read runs a single long-lived
 * segmentator task on {@code esql_external_io}; that task blocks on {@code chunkQueue.put},
 * {@code dispatchPermits.acquire}, {@code bufferPool.take}, and the upstream decompress {@code InputStream.read},
 * so it pins a pool thread for as long as the read is open. Its per-chunk parser tasks are submitted to the
 * <em>same</em> pool. When the number of concurrently-open segmentators reaches the pool size, every thread is
 * pinned by a segmentator that is itself waiting for a parser task to drain its queues — but those parser tasks
 * are stuck behind the segmentators in the pool's work queue and never get a thread. The off-pool consumers then
 * wait forever for pages that never arrive: a producer-side thread-footprint deadlock (elastic/esql-planning
 * #1093, structural-fix item 4), independent of the drain-side fix in #153074.
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
 */
final class StreamingSegmentatorAdmission {

    private final int maxConcurrentSegmentators;

    /** Segmentators currently handed to the pool (running or queued in the pool's own work queue). Guarded by {@code this}. */
    private int running = 0;
    /** Segmentators admitted here but not yet handed to the pool because the budget was full. Guarded by {@code this}. */
    private final ArrayDeque<Deferred> pending = new ArrayDeque<>();

    private record Deferred(Runnable segmentator, Executor executor, Consumer<RejectedExecutionException> onReject) {}

    /**
     * An effectively-unbounded controller ({@link Integer#MAX_VALUE} budget) that dispatches every segmentator
     * immediately. Test/benchmark-only: use on isolated, generously-sized pools where the saturation hazard cannot
     * arise. Production always constructs a controller with a real, pool-derived cap.
     */
    static StreamingSegmentatorAdmission unbounded() {
        return new StreamingSegmentatorAdmission(Integer.MAX_VALUE);
    }

    StreamingSegmentatorAdmission(int maxConcurrentSegmentators) {
        this.maxConcurrentSegmentators = Math.max(1, maxConcurrentSegmentators);
    }

    /**
     * Admits a segmentator: runs it on {@code executor} immediately if the concurrency budget allows, otherwise
     * queues it until a running segmentator completes. {@code onReject} runs (on some pool thread, possibly later)
     * if the executor refuses the task — the coordinator uses it to record the failure and wake its consumer,
     * exactly as the pre-admission direct-{@code execute} path did on a {@link RejectedExecutionException}. All
     * coordinators sharing this controller submit to the same node-level pool, so the executor is stable across
     * calls; it is passed per call so the controller need not hold a reference to it.
     */
    void submit(Runnable segmentator, Executor executor, Consumer<RejectedExecutionException> onReject) {
        Deferred toDispatch;
        synchronized (this) {
            if (running < maxConcurrentSegmentators) {
                running++;
                toDispatch = new Deferred(segmentator, executor, onReject);
            } else {
                pending.add(new Deferred(segmentator, executor, onReject));
                return;
            }
        }
        dispatch(toDispatch);
    }

    /**
     * Hands {@code d} to its executor, wrapping it so the slot is released and the next pending segmentator promoted
     * when it completes. On a {@link RejectedExecutionException} the reserved slot is freed and any promoted-then-
     * rejected successor is handled in the same loop, so a cascade of rejections (e.g. pool shutdown) unwinds
     * iteratively rather than recursively.
     */
    private void dispatch(Deferred d) {
        while (d != null) {
            try {
                Deferred toRun = d;
                toRun.executor().execute(() -> {
                    try {
                        toRun.segmentator().run();
                    } finally {
                        dispatch(releaseAndPromote());
                    }
                });
                return;
            } catch (RejectedExecutionException e) {
                d.onReject().accept(e);
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
}
