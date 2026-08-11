/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.utils;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Releasable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Applies a function to a list of items with bounded parallelism, collecting results in input order.
 *
 * <p>Work is routed through a {@link ThrottledTaskRunner}, which submits at most {@code maxConcurrency}
 * runnables to the provided executor at any one time and holds the remaining items in its own in-memory
 * FIFO queue. This bounds <em>submission</em>, not just execution: a large input list can never overflow
 * a bounded executor queue (e.g. the {@code SEARCH} pool), so it can only ever fail fast on a genuine
 * running-slot rejection — never flood the pool with thousands of enqueued tasks. The calling thread
 * blocks until all items complete (join pattern), then returns the results in the same order as the
 * input list. If the calling thread is interrupted while waiting, it short-circuits the not-yet-started
 * items and still drains the in-flight ones before failing, so no task is left running against the
 * executor once {@code gather} returns.
 *
 * <p>Fast-fail semantics: once any item throws, remaining not-yet-started items are skipped (they are
 * still dequeued by the runner but short-circuit before invoking the function). The first exception is
 * re-thrown after all tasks settle. Additional exceptions from other tasks are suppressed onto the first.
 * A running-slot rejection from the executor (surfaced via {@link ActionListener#onFailure}) is recorded
 * the same way, so the call fails fast and cleanly rather than hanging.
 *
 * <p>Fast-fail granularity is per slot, not per call: an item whose {@code fn} invocation is already in
 * progress when the failure (or a cancellation observed inside {@code fn}) is seen runs to completion —
 * at most {@code maxConcurrency} such calls can be in flight. Only items that have not yet started are
 * skipped. A caller that aborts via a cancellation check inside {@code fn} therefore stops promptly, but
 * up to one already-started call per slot may still finish before {@code gather} returns.
 *
 * <p>For zero or one items, the function is executed inline on the calling thread — no thread
 * dispatch occurs. This avoids executor overhead for the common trivial case.
 */
public final class BoundedParallelGather {

    private static final String TASK_RUNNER_NAME = "bounded-parallel-gather";

    private BoundedParallelGather() {}

    /**
     * Applies {@code fn} to each item in {@code items} with at most {@code maxConcurrency}
     * calls in flight simultaneously. Results are returned in the same order as the input list.
     *
     * @param items          the inputs to process; must not be null
     * @param fn             the function to apply to each input; may throw any exception
     * @param maxConcurrency maximum number of concurrent calls; must be {@code >= 1}
     * @param executor       the executor used to dispatch work
     * @param <T>            input type
     * @param <R>            result type
     * @return a list of results in the same order as {@code items}
     * @throws Exception     the first exception thrown by any invocation of {@code fn} (or by the
     *                       executor when rejecting a running slot), with remaining exceptions
     *                       suppressed onto it
     */
    public static <T, R> List<R> gather(List<T> items, CheckedFunction<T, R, Exception> fn, int maxConcurrency, Executor executor)
        throws Exception {
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be >= 1, got: " + maxConcurrency);
        }

        int size = items.size();
        if (size == 0) {
            return List.of();
        }

        // For a single item, run inline — avoids executor overhead and keeps stack traces clean.
        if (size == 1) {
            return List.of(fn.apply(items.get(0)));
        }

        Object[] results = new Object[size];
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicReference<Exception> firstError = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(size);

        // The runner only ever dispatches up to maxConcurrency runnables to the executor, so enqueue
        // rejection is impossible. Any rejection here is a running-slot rejection (the executor is
        // already saturated by real work) and arrives per-task via onFailure.
        ThrottledTaskRunner runner = new ThrottledTaskRunner(TASK_RUNNER_NAME, Math.min(size, maxConcurrency), executor);

        for (int i = 0; i < size; i++) {
            final int idx = i;
            final T item = items.get(i);
            runner.enqueueTask(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    // Closing the releasable lets the runner dispatch the next queued task; do it
                    // before counting down so a freed slot is observed promptly.
                    try (releasable) {
                        if (failed.get() == false) {
                            try {
                                results[idx] = fn.apply(item);
                            } catch (Exception e) {
                                recordError(failed, firstError, e);
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // Reached when the executor rejects this running slot (EsAbortPolicy routes the
                    // rejection here rather than throwing out of enqueueTask). The runner closes the
                    // releasable for this path itself, so we only record and release the latch.
                    try {
                        recordError(failed, firstError, e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        // Block until every enqueued task has settled. If the calling thread is interrupted we must NOT
        // return early: the tasks are already enqueued/running on the executor and would otherwise keep
        // reading footers (and writing into the shared results array) after this method has returned,
        // breaking the synchronous contract. Instead, record the interruption (which sets the failed flag
        // so not-yet-started tasks short-circuit and the queue drains without doing more work), keep
        // waiting for the in-flight tasks to settle, then restore the interrupt status and fail. The
        // runner guarantees every enqueued task completes via onResponse/onFailure, so the latch always
        // reaches zero.
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
                recordError(failed, firstError, new RuntimeException("Interrupted while gathering results", e));
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        Exception error = firstError.get();
        if (error != null) {
            throw error;
        }

        List<R> resultList = new ArrayList<>(size);
        for (Object result : results) {
            @SuppressWarnings("unchecked")
            R typed = (R) result;
            resultList.add(typed);
        }
        return resultList;
    }

    private static void recordError(AtomicBoolean failed, AtomicReference<Exception> firstError, Exception e) {
        failed.set(true);
        if (firstError.compareAndSet(null, e) == false) {
            firstError.get().addSuppressed(e);
        }
    }
}
