/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe counter struct for {@link OrcFormatReader}; {@link #snapshot()} yields the immutable
 * typed {@link OrcReaderStatus}. ORC's internal stripe rejection (stats / dictionary / bloom) is not
 * exposed by the Reader API, so selectivity must be inferred from {@code rows_emitted} vs.
 * {@code stripes_total}.
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

    // Footer cache (JVM-wide ParsedFooterCache)
    private final LongAdder footerCacheHits = new LongAdder();
    private final LongAdder footerCacheMisses = new LongAdder();

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
     * Records one footer-cache lookup: {@code hit == true} when the parsed ORC tail was reused,
     * {@code false} when this caller parsed and inserted it.
     */
    public void recordFooterCache(boolean hit) {
        if (hit) {
            footerCacheHits.increment();
        } else {
            footerCacheMisses.increment();
        }
    }

    public OrcReaderStatus snapshot() {
        List<String> sortedPredicates = predicateColumns.stream().sorted().toList();
        return new OrcReaderStatus(
            rowsEmitted.sum(),
            footerReadNanos.sum(),
            footerSizeBytes.sum(),
            footerCacheHits.sum(),
            footerCacheMisses.sum(),
            stripesInFile.sum(),
            stripesTotal.sum(),
            predicatePushdownUsed,
            sortedPredicates,
            columnsProjected,
            columnsTotal,
            totalReadNanos.sum()
        );
    }
}
