/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe counter struct for a {@link ParquetFormatReader}. One instance is held per
 * reader; instrumentation sites bump fields around footer reads, row-group filtering, page-index
 * narrowing, late materialization, per-column reads, the {@code read} / {@code readRange} entry
 * points, and the row-group transitions / per-batch decode of the returned column iterators
 * ({@code ParquetColumnIterator}, {@link OptimizedParquetColumnIterator}). {@link #snapshot()}
 * produces an immutable {@link Map} of counter values that can be folded into the operator-status
 * envelope.
 * <p>
 * Counter buckets: footer ({@code footer_read_nanos}, {@code footer_size_bytes},
 * {@code row_groups_in_file}), row-group filter ({@code row_groups_total}, {@code row_groups_kept}),
 * page index ({@code page_index_used}, {@code rows_in_kept_row_groups}, {@code rows_after_page_index}),
 * late materialization ({@code late_materialization_enabled},
 * {@code late_materialization_used}, {@code predicate_columns}),
 * aggregate ({@code read_nanos}) — producer-thread time across open, row-group transitions
 * (prefetch wait, filtering), and per-batch decode/decompress — and a typed per-column map under
 * {@code columns}.
 * <p>
 * The mutable / immutable split mirrors the {@link org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetricsCounters}
 * pattern. {@link LongAdder} is preferred over {@code AtomicLong} because async read-path
 * callbacks can update counters from worker threads concurrently with the operator status snapshot.
 */
public final class ParquetReaderCounters {

    // Footer
    private final LongAdder footerReadNanos = new LongAdder();
    private final LongAdder footerSizeBytes = new LongAdder();
    private final LongAdder rowGroupsInFile = new LongAdder();

    // Row-group filter
    private final LongAdder rowGroupsTotal = new LongAdder();
    private final LongAdder rowGroupsKept = new LongAdder();

    // Predicate pushdown (set when a non-null filter predicate was passed into the read path —
    // mirror of OrcReaderCounters.predicatePushdownUsed for cross-format consistency).
    private volatile boolean predicatePushdownUsed = false;

    // Page index
    private volatile boolean pageIndexUsed = false;
    private final LongAdder rowsInKeptRowGroups = new LongAdder();
    private final LongAdder rowsAfterPageIndex = new LongAdder();

    // Late materialization
    private volatile boolean lateMaterializationEnabled = false;
    private volatile boolean lateMaterializationUsed = false;
    private final Set<String> predicateColumns = ConcurrentHashMap.newKeySet();

    // Aggregate
    private final LongAdder rowsEmitted = new LongAdder();
    private final LongAdder totalReadNanos = new LongAdder();

    // Footer cache (JVM-wide ParsedFooterCache)
    private final LongAdder footerCacheHits = new LongAdder();
    private final LongAdder footerCacheMisses = new LongAdder();

    // Per-column. Concurrent for thread-safe lazy creation; values are mutated via PerColumnCounters
    // which itself uses LongAdder, so reads from multiple threads remain non-blocking.
    private final Map<String, PerColumnCounters> perColumn = new ConcurrentHashMap<>();

    public void addFooterRead(long nanos, long sizeBytes, long rowGroupCount) {
        if (nanos > 0) {
            footerReadNanos.add(nanos);
        }
        if (sizeBytes > 0) {
            footerSizeBytes.add(sizeBytes);
        }
        if (rowGroupCount > 0) {
            rowGroupsInFile.add(rowGroupCount);
        }
    }

    /**
     * Records one row group seen by the filter. {@code kept} is true when the row group survived
     * all filter levels and remains a candidate for column-index page filtering. parquet-mr does
     * not expose which level (stats / dictionary / bloom) rejected a group, so only the total and
     * the survivor count are tracked.
     */
    public void addRowGroupFiltered(boolean kept) {
        rowGroupsTotal.increment();
        if (kept) {
            rowGroupsKept.increment();
        }
    }

    public void markPredicatePushdownUsed() {
        predicatePushdownUsed = true;
    }

    public void markPageIndexUsed() {
        pageIndexUsed = true;
    }

    public void addPageIndexRows(long rowsInKept, long rowsAfter) {
        if (rowsInKept > 0) {
            rowsInKeptRowGroups.add(rowsInKept);
        }
        if (rowsAfter > 0) {
            rowsAfterPageIndex.add(rowsAfter);
        }
    }

    public void setLateMaterializationEnabled(boolean enabled) {
        lateMaterializationEnabled = enabled;
    }

    public void markLateMaterializationUsed() {
        lateMaterializationUsed = true;
    }

    public void addPredicateColumns(Collection<String> names) {
        if (names == null || names.isEmpty()) {
            return;
        }
        predicateColumns.addAll(names);
    }

    public void addRowsEmitted(long delta) {
        if (delta > 0) {
            rowsEmitted.add(delta);
        }
    }

    public void addTotalReadNanos(long nanos) {
        if (nanos > 0) {
            totalReadNanos.add(nanos);
        }
    }

    /**
     * Records one footer-cache lookup: {@code hit == true} when the parsed footer was reused,
     * {@code false} when this caller parsed and inserted it.
     */
    public void recordFooterCache(boolean hit) {
        if (hit) {
            footerCacheHits.increment();
        } else {
            footerCacheMisses.increment();
        }
    }

    /** Returns the per-column counters for {@code column}, creating them on first access. */
    public PerColumnCounters perColumn(String column) {
        return perColumn.computeIfAbsent(column, k -> new PerColumnCounters());
    }

    /**
     * Returns an immutable, typed snapshot of the current counter values. Per-column entries ride as
     * a {@code Map<String, PerColumnStatus>} — {@link PerColumnStatus} is itself {@code Writeable},
     * so it crosses the operator-status wire directly with no flattening.
     */
    public ParquetReaderStatus snapshot() {
        // Sort predicate columns for deterministic snapshots; insertion order is meaningless because
        // ConcurrentHashMap.newKeySet() is not insertion-ordered.
        List<String> sortedPredicates = predicateColumns.stream().sorted().toList();
        Map<String, PerColumnStatus> columnsSnap;
        if (perColumn.isEmpty()) {
            columnsSnap = Map.of();
        } else {
            Map<String, PerColumnStatus> m = new TreeMap<>();
            perColumn.forEach((k, v) -> m.put(k, v.snapshot()));
            columnsSnap = Collections.unmodifiableMap(m);
        }
        return new ParquetReaderStatus(
            rowsEmitted.sum(),
            predicatePushdownUsed,
            footerReadNanos.sum(),
            footerSizeBytes.sum(),
            footerCacheHits.sum(),
            footerCacheMisses.sum(),
            rowGroupsInFile.sum(),
            rowGroupsTotal.sum(),
            rowGroupsKept.sum(),
            pageIndexUsed,
            rowsInKeptRowGroups.sum(),
            rowsAfterPageIndex.sum(),
            lateMaterializationEnabled,
            lateMaterializationUsed,
            sortedPredicates,
            totalReadNanos.sum(),
            columnsSnap
        );
    }

    /** Mutable per-column summary. One instance per column path. */
    public static final class PerColumnCounters {
        private volatile String materialization;

        public void setMaterialization(String m) {
            materialization = m;
        }

        public PerColumnStatus snapshot() {
            return new PerColumnStatus(materialization);
        }
    }
}
