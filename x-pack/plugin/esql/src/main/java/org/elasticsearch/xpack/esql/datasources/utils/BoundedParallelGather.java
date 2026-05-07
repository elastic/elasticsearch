/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.utils;

import org.elasticsearch.core.CheckedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Applies a function to a list of items with bounded parallelism, collecting results in input order.
 *
 * <p>Dispatches work to the provided executor with a semaphore to cap the number of concurrent
 * in-flight calls. Blocks the calling thread until all items complete (join pattern), then
 * returns the results in the same order as the input list.
 *
 * <p>Fast-fail semantics: once any item throws, remaining not-yet-started items are skipped.
 * The first exception is re-thrown after all in-flight tasks complete. Additional exceptions
 * from other tasks are suppressed onto the first.
 *
 * <p>For zero or one items, the function is executed inline on the calling thread — no thread
 * dispatch occurs. This avoids executor overhead for the common trivial case.
 */
public final class BoundedParallelGather {

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
     * @throws Exception     the first exception thrown by any invocation of {@code fn},
     *                       with remaining exceptions suppressed onto it
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

        Semaphore semaphore = new Semaphore(Math.min(size, maxConcurrency));
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicReference<Exception> firstError = new AtomicReference<>();

        @SuppressWarnings("unchecked")
        CompletableFuture<R>[] futures = new CompletableFuture[size];

        for (int i = 0; i < size; i++) {
            T item = items.get(i);
            futures[i] = CompletableFuture.supplyAsync(() -> {
                if (failed.get()) {
                    return null;
                }
                try {
                    semaphore.acquire();
                    try {
                        if (failed.get()) {
                            return null;
                        }
                        return fn.apply(item);
                    } finally {
                        semaphore.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    Exception wrapped = new RuntimeException("Interrupted while processing item", e);
                    recordError(failed, firstError, wrapped);
                    throw new CompletionException(wrapped);
                } catch (Exception e) {
                    recordError(failed, firstError, e);
                    if (e instanceof RuntimeException re) {
                        throw re;
                    }
                    throw new CompletionException(e);
                }
            }, executor);
        }

        try {
            CompletableFuture.allOf(futures).join();
        } catch (CompletionException ignored) {
            // The real error is in firstError; we will rethrow below.
        }

        Exception error = firstError.get();
        if (error != null) {
            throw error;
        }

        List<R> results = new ArrayList<>(size);
        for (CompletableFuture<R> future : futures) {
            // All futures are complete at this point; get() will not block and
            // will not throw because firstError is null (all succeeded).
            try {
                results.add(future.get());
            } catch (Exception e) {
                // Should not happen since firstError is null, but propagate defensively.
                throw new RuntimeException("Unexpected error collecting gather results", e);
            }
        }
        return results;
    }

    private static void recordError(AtomicBoolean failed, AtomicReference<Exception> firstError, Exception e) {
        failed.set(true);
        if (firstError.compareAndSet(null, e) == false) {
            firstError.get().addSuppressed(e);
        }
    }
}
