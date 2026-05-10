/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe counter struct for {@link OrcFormatReader}. One instance per reader,
 * shared across the parallel {@code OrcPageIterator} segments. Iterators bump these counters;
 * {@link #snapshot()} produces the immutable {@link Map} folded into the operator-status envelope.
 * <p>
 * What's observable: stripe count we asked the reader to consider, SearchArgument pushdown flags,
 * column projection counts, total rows yielded, total read nanos. What's NOT observable: which
 * stripes the ORC library accepted vs. rejected via stats / dictionary / bloom filter — the public
 * Reader API doesn't surface that, so we don't either. Selectivity has to be inferred from
 * {@code rows_emitted} vs the row count implied by {@code stripes_total}.
 * <p>
 * {@link LongAdder} keeps concurrent updates from segment threads non-blocking. {@code volatile}
 * scalars handle "set-once" style fields like the projection counts and the pushdown flag.
 */
public final class OrcReaderCounters {

    private final LongAdder footerReadNanos = new LongAdder();
    private final LongAdder footerSizeBytes = new LongAdder();
    private final LongAdder stripesInFile = new LongAdder();
    private final LongAdder stripesTotal = new LongAdder();

    private volatile boolean predicatePushdownUsed = false;
    private final Set<String> predicateColumns = ConcurrentHashMap.newKeySet();

    private volatile int columnsProjected = 0;
    private volatile int columnsTotal = 0;

    private final LongAdder rowsEmitted = new LongAdder();
    private final LongAdder totalReadNanos = new LongAdder();

    public void addFooterRead(long nanos, long sizeBytes, long stripeCount) {
        if (nanos > 0) {
            footerReadNanos.add(nanos);
        }
        if (sizeBytes > 0) {
            footerSizeBytes.add(sizeBytes);
        }
        if (stripeCount > 0) {
            stripesInFile.add(stripeCount);
        }
    }

    public void addStripesTotal(long delta) {
        if (delta > 0) {
            stripesTotal.add(delta);
        }
    }

    public void markPredicatePushdownUsed() {
        predicatePushdownUsed = true;
    }

    public void addPredicateColumns(Iterable<String> names) {
        if (names == null) {
            return;
        }
        for (String name : names) {
            if (name != null && name.isEmpty() == false) {
                predicateColumns.add(name);
            }
        }
    }

    public void setColumnCounts(int projected, int total) {
        if (projected >= 0) {
            columnsProjected = projected;
        }
        if (total >= 0) {
            columnsTotal = total;
        }
    }

    public void addRowsEmitted(long delta) {
        if (delta > 0) {
            rowsEmitted.add(delta);
        }
    }

    public void addReadNanos(long nanos) {
        if (nanos > 0) {
            totalReadNanos.add(nanos);
        }
    }

    /**
     * Returns an immutable snapshot of the current counter values. Keys: {@code format} (discriminator),
     * {@code rows_emitted} (rows the iterator yielded — same key name across all four format readers
     * for cross-format consumer aggregation), {@code footer_read_nanos}, {@code footer_size_bytes},
     * {@code stripes_in_file}, {@code stripes_total}, {@code predicate_pushdown_used},
     * {@code predicate_columns} (sorted), {@code columns_projected}, {@code columns_total},
     * {@code read_nanos}.
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("format", "orc");
        snap.put("rows_emitted", rowsEmitted.sum());
        snap.put("footer_read_nanos", footerReadNanos.sum());
        snap.put("footer_size_bytes", footerSizeBytes.sum());
        snap.put("stripes_in_file", stripesInFile.sum());
        snap.put("stripes_total", stripesTotal.sum());
        snap.put("predicate_pushdown_used", predicatePushdownUsed);
        // Sorted, deduplicated, immutable for stable output across snapshots.
        Set<String> sortedPredicates = new LinkedHashSet<>();
        predicateColumns.stream().sorted().forEach(sortedPredicates::add);
        snap.put("predicate_columns", Collections.unmodifiableSet(sortedPredicates));
        snap.put("columns_projected", (long) columnsProjected);
        snap.put("columns_total", (long) columnsTotal);
        snap.put("read_nanos", totalReadNanos.sum());
        return Map.copyOf(snap);
    }
}
