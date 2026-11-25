/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-bound sink for captured per-file source statistics. The text-format readers' close hooks call
 * {@link #record} with one flat {@code _stats.*} map per contribution; whichever operator drove the
 * iteration binds a sink ahead of time so the contributions end up on the operator's {@code Status}
 * and ride back to the coordinator via {@link org.elasticsearch.compute.operator.DriverCompletionInfo}.
 * <p>
 * In the orthogonal stripe model the READER owns stripe addressing: it attributes each record to its
 * canonical stripe ({@code ordinal = floor(recordStartOffset / stripeSize)}, using the base offset and
 * grid passed via {@code FormatReadContext}) and emits one {@link #record} call per stripe it touched,
 * each map already carrying the {@code _stats.stripe_*} addressing keys. This sink no longer stamps
 * coverage — there is nothing for the coordinator to add, because only the reader knows where each
 * record sits relative to the grid.
 * <p>
 * Sinks must be {@linkplain #bind bound} on the same thread that subsequently invokes the reader's
 * {@code close()}. For the synchronous operator the operator owns the iteration thread; for the async
 * path the coordinator binds on the reader/worker thread that runs the iterator.
 */
public final class ExternalStatsCapture {

    private static final ThreadLocal<ConcurrentMap<String, List<Map<String, Object>>>> ACTIVE = new ThreadLocal<>();

    private ExternalStatsCapture() {}

    /**
     * Appends a flat {@code _stats.*} contribution for {@code filePath}. Multiple contributions per
     * path accumulate (one per stripe a read covered, one per whole-file read); the coordinator-side
     * reconciler routes them by their marker keys. No-op if no sink is bound on the current thread, if
     * the path is {@code null}, or if the map is {@code null}/empty.
     * <p>
     * A parallel read's chunks do not reach this method one by one: they hand their pieces of a stripe to a
     * {@link StripeStatsStaging}, which calls {@link #recordTo} once the stripe is covered.
     * <p>
     * The sink is typed as {@link ConcurrentMap} because parallel-parsing workers concurrently invoke
     * {@code computeIfAbsent} on the outer map; only the {@link ConcurrentMap} contract makes that
     * lookup-or-insert atomic.
     */
    public static void record(String filePath, Map<String, Object> stats) {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ACTIVE.get();
        if (sink != null) {
            recordTo(sink, filePath, stats);
        }
    }

    /**
     * Appends a contribution to a sink named outright rather than found on the current thread. Used by
     * {@link StripeStatsStaging}, and needed for {@link StripeStatsStaging#flush}: a stripe staged from
     * {@code contribute} runs on a parser thread, which both coordinators DO bind — they bind around the
     * per-chunk {@code reader.read(...).close()} they drive — but the flush runs on the thread that closes the
     * read, which nothing bound. This is the one definition of the append, so both entry points share the
     * list's identity and locking.
     */
    static void recordTo(ConcurrentMap<String, List<Map<String, Object>>> sink, String filePath, Map<String, Object> stats) {
        if (filePath == null || stats == null || stats.isEmpty()) {
            return;
        }
        sink.computeIfAbsent(filePath, k -> Collections.synchronizedList(new ArrayList<>())).add(stats);
    }

    /**
     * Binds {@code sink} as the active capture target on the current thread; returns a handle the
     * caller must close (try-with-resources) to restore the previous sink. The same sink instance can
     * also be polled directly by the binding owner — {@link #record} only writes.
     */
    public static Handle bind(ConcurrentMap<String, List<Map<String, Object>>> sink) {
        ConcurrentMap<String, List<Map<String, Object>>> previous = ACTIVE.get();
        ACTIVE.set(sink);
        return () -> {
            if (previous == null) {
                ACTIVE.remove();
            } else {
                ACTIVE.set(previous);
            }
        };
    }

    /** Convenience factory for a fresh thread-safe sink. */
    public static ConcurrentMap<String, List<Map<String, Object>>> newSink() {
        return new ConcurrentHashMap<>();
    }

    @FunctionalInterface
    public interface Handle extends AutoCloseable {
        @Override
        void close();
    }
}
