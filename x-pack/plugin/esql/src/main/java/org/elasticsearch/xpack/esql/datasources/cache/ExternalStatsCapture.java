/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-bound sink for captured per-file source statistics. The text-format iterators' close
 * hooks call {@link #record} with a flat {@code _stats.*} map describing the file they finished
 * draining; whichever operator drove the iteration binds a sink ahead of time so the contributions
 * end up on the operator's {@code Status} and ride back to the coordinator via
 * {@link org.elasticsearch.compute.operator.DriverCompletionInfo}.
 * <p>
 * Sinks must be {@linkplain #bind bound} on the same thread that subsequently invokes the
 * iterator's {@code close()}. For the synchronous {@code ExternalSourceOperator} the operator owns
 * the iteration thread; for the async path {@code AsyncExternalSourceBuffer} binds on the reader
 * thread that runs the iterator.
 */
public final class ExternalStatsCapture {

    private static final ThreadLocal<Map<String, Map<String, Object>>> ACTIVE = new ThreadLocal<>();

    private ExternalStatsCapture() {}

    /**
     * Records a flat {@code _stats.*} contribution for {@code filePath}. No-op if no sink is bound
     * on the current thread, if the path is {@code null}, or if the map is {@code null}/empty.
     */
    public static void record(String filePath, Map<String, Object> stats) {
        if (filePath == null || stats == null || stats.isEmpty()) {
            return;
        }
        Map<String, Map<String, Object>> sink = ACTIVE.get();
        if (sink != null) {
            sink.put(filePath, Map.copyOf(stats));
        }
    }

    /**
     * Binds {@code sink} as the active capture target on the current thread; returns a handle the
     * caller must close (try-with-resources) to restore the previous sink. The same sink instance
     * can also be polled directly by the binding owner — {@link #record} only writes; the snapshot
     * is the sink's own responsibility.
     */
    public static Handle bind(Map<String, Map<String, Object>> sink) {
        Map<String, Map<String, Object>> previous = ACTIVE.get();
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
    public static Map<String, Map<String, Object>> newSink() {
        return new ConcurrentHashMap<>();
    }

    @FunctionalInterface
    public interface Handle extends AutoCloseable {
        @Override
        void close();
    }
}
