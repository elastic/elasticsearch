/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OrcReaderCountersTests extends ESTestCase {

    public void testEmptySnapshot() {
        OrcReaderCounters counters = new OrcReaderCounters();
        Map<String, Object> snap = counters.snapshot();
        assertEquals(0L, snap.get("stripes_total"));
        assertEquals(false, snap.get("predicate_pushdown_used"));
        assertEquals(Set.of(), snap.get("predicate_columns"));
        assertEquals(0L, snap.get("columns_projected"));
        assertEquals(0L, snap.get("columns_total"));
        assertEquals(0L, snap.get("rows_emitted"));
        assertEquals(0L, snap.get("read_nanos"));
        assertEquals(0L, snap.get("footer_cache_hits"));
        assertEquals(0L, snap.get("footer_cache_misses"));
    }

    public void testFooterCacheHitMissCounters() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.recordFooterCacheMiss();
        counters.recordFooterCacheHit();
        counters.recordFooterCacheHit();

        Map<String, Object> snap = counters.snapshot();
        assertEquals(2L, snap.get("footer_cache_hits"));
        assertEquals(1L, snap.get("footer_cache_misses"));
    }

    public void testSnapshotReflectsAllIncrements() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.addStripesTotal(8);
        counters.markPredicatePushdownUsed();
        counters.addPredicateColumns(List.of("level", "host"));
        counters.setColumnCounts(3, 12);
        counters.addRowsEmitted(2_500);
        counters.addReadNanos(987_654_321L);

        Map<String, Object> snap = counters.snapshot();
        assertEquals(8L, snap.get("stripes_total"));
        assertEquals(true, snap.get("predicate_pushdown_used"));
        assertEquals(3L, snap.get("columns_projected"));
        assertEquals(12L, snap.get("columns_total"));
        assertEquals(2_500L, snap.get("rows_emitted"));
        assertEquals(987_654_321L, snap.get("read_nanos"));
        // predicate_columns is sorted — alphabetical order regardless of insertion order.
        @SuppressWarnings("unchecked")
        Set<String> predicates = (Set<String>) snap.get("predicate_columns");
        assertEquals(List.of("host", "level"), predicates.stream().toList());
    }

    public void testNonPositiveDeltasIgnored() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.addStripesTotal(0);
        counters.addStripesTotal(-3);
        counters.addRowsEmitted(0);
        counters.addReadNanos(-5);
        Map<String, Object> snap = counters.snapshot();
        assertEquals(0L, snap.get("stripes_total"));
        assertEquals(0L, snap.get("rows_emitted"));
        assertEquals(0L, snap.get("read_nanos"));
    }

    public void testNullAndEmptyPredicateColumnsTolerated() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.addPredicateColumns(null); // ignored
        counters.addPredicateColumns(List.of("", "ok"));
        Map<String, Object> snap = counters.snapshot();
        @SuppressWarnings("unchecked")
        Set<String> predicates = (Set<String>) snap.get("predicate_columns");
        assertEquals(Set.of("ok"), predicates);
    }

    public void testColumnCountsClampNegativesToCurrent() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.setColumnCounts(5, 10);
        counters.setColumnCounts(-1, -1); // both negative — both ignored
        Map<String, Object> snap = counters.snapshot();
        assertEquals(5L, snap.get("columns_projected"));
        assertEquals(10L, snap.get("columns_total"));
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
        Map<String, Object> snap = counters.snapshot();
        assertEquals(expectedStripes, snap.get("stripes_total"));
        assertEquals(expectedRows, snap.get("rows_emitted"));
        assertEquals(expectedNanos, snap.get("read_nanos"));
    }

    public void testSnapshotIsImmutable() {
        OrcReaderCounters counters = new OrcReaderCounters();
        counters.addStripesTotal(3);
        Map<String, Object> snap = counters.snapshot();
        counters.addStripesTotal(5);
        assertEquals(3L, snap.get("stripes_total"));
        expectThrows(UnsupportedOperationException.class, () -> snap.put("stripes_total", 0L));
    }
}
