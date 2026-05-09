/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe counter struct for a {@link ParquetFormatReader}. One instance is held per
 * reader; instrumentation sites bump fields around footer reads, row-group filtering, page-index
 * narrowing, late materialization, per-column reads, and the {@code read} / {@code readRange}
 * entry points. {@link #snapshot()} produces an immutable {@link Map} of counter values that can
 * be folded into the operator-status envelope.
 * <p>
 * Counter buckets: footer ({@code footer_read_nanos}, {@code footer_size_bytes},
 * {@code row_groups_in_file}), row-group filter ({@code row_groups_total},
 * {@code row_groups_passed_stats}, {@code row_groups_passed_dictionary},
 * {@code row_groups_passed_bloom}, {@code row_groups_kept}), page index
 * ({@code page_index_used}, {@code rows_in_kept_row_groups}, {@code rows_after_page_index}),
 * late materialization ({@code late_materialization_enabled},
 * {@code late_materialization_used}, {@code predicate_columns}),
 * aggregate ({@code total_read_nanos}), and a typed per-column map under {@code columns}.
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
    private final LongAdder rowGroupsPassedStats = new LongAdder();
    private final LongAdder rowGroupsPassedDictionary = new LongAdder();
    private final LongAdder rowGroupsPassedBloom = new LongAdder();
    private final LongAdder rowGroupsKept = new LongAdder();

    // Page index
    private volatile boolean pageIndexUsed = false;
    private final LongAdder rowsInKeptRowGroups = new LongAdder();
    private final LongAdder rowsAfterPageIndex = new LongAdder();

    // Late materialization
    private volatile boolean lateMaterializationEnabled = false;
    private volatile boolean lateMaterializationUsed = false;
    private final Set<String> predicateColumns = ConcurrentHashMap.newKeySet();

    // Aggregate
    private final LongAdder totalReadNanos = new LongAdder();

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
     * Records a single row-group filter pass: each {@code passed*} flag is true when the row group
     * survived the corresponding filter level. {@code kept} is true when the row group survives all
     * three levels and remains a candidate for column-index page filtering.
     */
    public void addRowGroupFiltered(boolean passedStats, boolean passedDictionary, boolean passedBloom, boolean kept) {
        rowGroupsTotal.increment();
        if (passedStats) {
            rowGroupsPassedStats.increment();
        }
        if (passedDictionary) {
            rowGroupsPassedDictionary.increment();
        }
        if (passedBloom) {
            rowGroupsPassedBloom.increment();
        }
        if (kept) {
            rowGroupsKept.increment();
        }
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

    public void addTotalReadNanos(long nanos) {
        if (nanos > 0) {
            totalReadNanos.add(nanos);
        }
    }

    /** Returns the per-column counters for {@code column}, creating them on first access. */
    public PerColumnCounters perColumn(String column) {
        return perColumn.computeIfAbsent(column, k -> new PerColumnCounters());
    }

    /**
     * Returns an immutable snapshot of the current counter values. Per-column entries are captured
     * under the {@code "columns"} key as {@code Map<String, Map<String, Object>>}: the outer
     * envelope ({@code AsyncExternalSourceOperator.Status.formatReader}) ships through
     * {@code StreamOutput.writeGenericMap}, which only supports leaf types in the {@code WRITERS}
     * registry, so each {@link PerColumnStatus} is flattened via {@link PerColumnStatus#toMap()}
     * before it crosses the carrier boundary.
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("footer_read_nanos", footerReadNanos.sum());
        snap.put("footer_size_bytes", footerSizeBytes.sum());
        snap.put("row_groups_in_file", rowGroupsInFile.sum());

        snap.put("row_groups_total", rowGroupsTotal.sum());
        snap.put("row_groups_passed_stats", rowGroupsPassedStats.sum());
        snap.put("row_groups_passed_dictionary", rowGroupsPassedDictionary.sum());
        snap.put("row_groups_passed_bloom", rowGroupsPassedBloom.sum());
        snap.put("row_groups_kept", rowGroupsKept.sum());

        snap.put("page_index_used", pageIndexUsed);
        snap.put("rows_in_kept_row_groups", rowsInKeptRowGroups.sum());
        snap.put("rows_after_page_index", rowsAfterPageIndex.sum());

        snap.put("late_materialization_enabled", lateMaterializationEnabled);
        snap.put("late_materialization_used", lateMaterializationUsed);
        // Sort predicate columns for deterministic snapshots; insertion order is meaningless because
        // ConcurrentHashMap.newKeySet() is not insertion-ordered.
        Set<String> sortedPredicates = new LinkedHashSet<>();
        predicateColumns.stream().sorted().forEach(sortedPredicates::add);
        snap.put("predicate_columns", sortedPredicates);

        snap.put("total_read_nanos", totalReadNanos.sum());

        if (perColumn.isEmpty() == false) {
            Map<String, Map<String, Object>> columnsSnap = new LinkedHashMap<>();
            perColumn.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> columnsSnap.put(e.getKey(), e.getValue().snapshot().toMap()));
            snap.put("columns", Collections.unmodifiableMap(columnsSnap));
        }
        return Map.copyOf(snap);
    }

    /** Mutable per-column counter struct. One instance per column path. */
    public static final class PerColumnCounters {
        private final LongAdder bytesCompressedRead = new LongAdder();
        private final LongAdder bytesDecompressed = new LongAdder();
        private final LongAdder decompressionNanos = new LongAdder();
        private final LongAdder decodeNanos = new LongAdder();
        private final LongAdder pagesRead = new LongAdder();
        private volatile String materialization;

        public void addBytesCompressedRead(long bytes) {
            if (bytes > 0) {
                bytesCompressedRead.add(bytes);
            }
        }

        public void addBytesDecompressed(long bytes) {
            if (bytes > 0) {
                bytesDecompressed.add(bytes);
            }
        }

        public void addDecompressionNanos(long nanos) {
            if (nanos > 0) {
                decompressionNanos.add(nanos);
            }
        }

        public void addDecodeNanos(long nanos) {
            if (nanos > 0) {
                decodeNanos.add(nanos);
            }
        }

        public void addPagesRead(long pages) {
            if (pages > 0) {
                pagesRead.add(pages);
            }
        }

        public void setMaterialization(String m) {
            materialization = m;
        }

        public PerColumnStatus snapshot() {
            return new PerColumnStatus(
                bytesCompressedRead.sum(),
                bytesDecompressed.sum(),
                decompressionNanos.sum(),
                decodeNanos.sum(),
                pagesRead.sum(),
                materialization
            );
        }
    }
}
