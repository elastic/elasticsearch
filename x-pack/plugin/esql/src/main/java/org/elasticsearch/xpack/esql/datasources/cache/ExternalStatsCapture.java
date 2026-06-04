/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

    private static final ThreadLocal<ConcurrentMap<String, List<Map<String, Object>>>> ACTIVE = new ThreadLocal<>();
    /**
     * Coverage bound alongside {@link #ACTIVE} so every contribution a worker records while reading
     * one chunk/segment is stamped with the file byte-range it observed ({@link
     * ExternalStats#COVERAGE_START_KEY} / {@link ExternalStats#COVERAGE_END_KEY} / {@link
     * ExternalStats#COVERAGE_IS_LAST_KEY}). The reconciler unions contributions by that range, so a
     * range re-observed by another scan of the same file (e.g. a sibling FORK branch) is counted once
     * while disjoint ranges are summed. {@code null} for whole-file reads, which stay un-addressed and
     * keep the authoritative {@code WholeFile} dedup path.
     */
    private static final ThreadLocal<Coverage> ACTIVE_COVERAGE = new ThreadLocal<>();

    /** The file byte-range a chunk/segment observed, in that path's read coordinate system. */
    public record Coverage(long start, long end, boolean last) {}

    private ExternalStatsCapture() {}

    /**
     * Appends a flat {@code _stats.*} contribution for {@code filePath}. Multiple contributions
     * per path accumulate (one per parallel-parsing chunk, one per macro-split, one per whole-file
     * read); the coordinator-side merger combines them via {@code SourceStatisticsSerializer
     * .mergeStatistics}. No-op if no sink is bound on the current thread, if the path is
     * {@code null}, or if the map is {@code null}/empty.
     * <p>
     * The sink is typed as {@link ConcurrentMap} because parallel-parsing workers concurrently
     * invoke {@code computeIfAbsent} on the outer map; only the {@link ConcurrentMap} contract
     * makes that lookup-or-insert atomic.
     */
    public static void record(String filePath, Map<String, Object> stats) {
        if (filePath == null || stats == null || stats.isEmpty()) {
            return;
        }
        ConcurrentMap<String, List<Map<String, Object>>> sink = ACTIVE.get();
        if (sink != null) {
            // Stamp the active coverage range so the coordinator can union this chunk/segment by the
            // bytes it observed. Copy rather than mutate in place — the caller may pass an immutable or
            // reused map, and coverage must not leak back into a caller's buffer. Skip when a
            // contribution already carries coverage.
            Coverage coverage = ACTIVE_COVERAGE.get();
            Map<String, Object> stamped = stats;
            if (coverage != null && stats.containsKey(ExternalStats.COVERAGE_START_KEY) == false) {
                stamped = new HashMap<>(stats);
                stamped.put(ExternalStats.COVERAGE_START_KEY, coverage.start());
                stamped.put(ExternalStats.COVERAGE_END_KEY, coverage.end());
                stamped.put(ExternalStats.COVERAGE_IS_LAST_KEY, coverage.last());
            }
            Map<String, Object> contribution = stamped;
            sink.computeIfAbsent(filePath, k -> Collections.synchronizedList(new ArrayList<>())).add(contribution);
        }
    }

    /**
     * Binds {@code sink} as the active capture target on the current thread; returns a handle the
     * caller must close (try-with-resources) to restore the previous sink. The same sink instance
     * can also be polled directly by the binding owner — {@link #record} only writes; the snapshot
     * is the sink's own responsibility.
     */
    public static Handle bind(ConcurrentMap<String, List<Map<String, Object>>> sink) {
        return bind(sink, null);
    }

    /**
     * Binds {@code sink} and the {@code coverage} this read observes on the current thread; every
     * {@link #record} call made under this binding is stamped with the coverage range (see {@link
     * ExternalStats#COVERAGE_START_KEY}) so the coordinator can union contributions by the bytes they
     * cover. Bind one coverage per chunk/segment read; pass {@code null} for a whole-file read (which
     * stays on the authoritative {@code WholeFile} dedup path). The returned handle restores both the
     * previous sink and the previous coverage.
     */
    public static Handle bind(ConcurrentMap<String, List<Map<String, Object>>> sink, Coverage coverage) {
        ConcurrentMap<String, List<Map<String, Object>>> previousSink = ACTIVE.get();
        Coverage previousCoverage = ACTIVE_COVERAGE.get();
        ACTIVE.set(sink);
        if (coverage != null) {
            ACTIVE_COVERAGE.set(coverage);
        }
        return () -> {
            if (previousSink == null) {
                ACTIVE.remove();
            } else {
                ACTIVE.set(previousSink);
            }
            if (previousCoverage == null) {
                ACTIVE_COVERAGE.remove();
            } else {
                ACTIVE_COVERAGE.set(previousCoverage);
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
