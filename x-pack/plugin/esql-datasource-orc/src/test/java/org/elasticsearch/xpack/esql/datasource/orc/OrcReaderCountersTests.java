/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OrcReaderCountersTests extends ESTestCase {

    public void testEmptySnapshot() {
        OrcReaderCounters counters = new OrcReaderCounters();
        var snap = counters.snapshot();
        assertEquals(0L, snap.stripesTotal());
        assertEquals(false, snap.predicatePushdownUsed());
        assertEquals(List.of(), snap.predicateColumns());
        assertEquals(0L, snap.columnsProjected());
        assertEquals(0L, snap.columnsTotal());
        assertEquals(0L, snap.rowsEmitted());
        assertEquals(0L, snap.readNanos());
        assertEquals(0L, snap.footerCacheHits());
        assertEquals(0L, snap.footerCacheMisses());
    }

    public void testFooterCacheHitMissCounters() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.recordFooterCache(false);
        counters.recordFooterCache(true);
        counters.recordFooterCache(true);

        var snap = counters.snapshot();
        assertEquals(2L, snap.footerCacheHits());
        assertEquals(1L, snap.footerCacheMisses());
    }

    public void testSnapshotReflectsAllIncrements() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.addStripesTotal(8);
        counters.markPredicatePushdownUsed();
        counters.addPredicateColumns(List.of("level", "host"));
        counters.setColumnCounts(3, 12);
        counters.addRowsEmitted(2_500);
        counters.addReadNanos(987_654_321L);

        var snap = counters.snapshot();
        assertEquals(8L, snap.stripesTotal());
        assertEquals(true, snap.predicatePushdownUsed());
        assertEquals(3L, snap.columnsProjected());
        assertEquals(12L, snap.columnsTotal());
        assertEquals(2_500L, snap.rowsEmitted());
        assertEquals(987_654_321L, snap.readNanos());
        // predicate_columns is sorted — alphabetical order regardless of insertion order.
        List<String> predicates = snap.predicateColumns();
        assertEquals(List.of("host", "level"), predicates.stream().toList());
    }

    public void testNonPositiveDeltasIgnored() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.addStripesTotal(0);
        counters.addStripesTotal(-3);
        counters.addRowsEmitted(0);
        counters.addReadNanos(-5);
        var snap = counters.snapshot();
        assertEquals(0L, snap.stripesTotal());
        assertEquals(0L, snap.rowsEmitted());
        assertEquals(0L, snap.readNanos());
    }

    public void testNullAndEmptyPredicateColumnsTolerated() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.addPredicateColumns(null); // ignored
        counters.addPredicateColumns(List.of("", "ok"));
        var snap = counters.snapshot();
        List<String> predicates = snap.predicateColumns();
        assertEquals(List.of("ok"), predicates);
    }

    public void testColumnCountsClampNegativesToCurrent() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.setColumnCounts(5, 10);
        counters.setColumnCounts(-1, -1); // both negative — both ignored
        var snap = counters.snapshot();
        assertEquals(5L, snap.columnsProjected());
        assertEquals(10L, snap.columnsTotal());
    }

    public void testConcurrentIncrementsAccumulateWithoutLoss() throws Exception {
        OrcReaderCounters counters = new OrcReaderCounters();
        int threads = 8;
        int iterationsPerThread = 1_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        counters.addStripesTotal(2);
                        counters.addRowsEmitted(100);
                        counters.addReadNanos(75);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        long expectedStripes = (long) threads * iterationsPerThread * 2;
        long expectedRows = (long) threads * iterationsPerThread * 100;
        long expectedNanos = (long) threads * iterationsPerThread * 75;
        var snap = counters.snapshot();
        assertEquals(expectedStripes, snap.stripesTotal());
        assertEquals(expectedRows, snap.rowsEmitted());
        assertEquals(expectedNanos, snap.readNanos());
    }

    public void testSnapshotIsImmutable() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.addStripesTotal(3);
        var snap = counters.snapshot();
        counters.addStripesTotal(5);
        assertEquals(3L, snap.stripesTotal());
    }
}
