/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ParquetReaderCountersTests extends ESTestCase {

    public void testEmptySnapshotShape() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        var snap = c.snapshot();

        // Footer
        assertEquals(0L, snap.footerReadNanos());
        assertEquals(0L, snap.footerSizeBytes());
        assertEquals(0L, snap.footerCacheHits());
        assertEquals(0L, snap.footerCacheMisses());
        assertEquals(0L, snap.rowGroupsInFile());
        // Row-group filter
        assertEquals(0L, snap.rowGroupsTotal());
        assertEquals(0L, snap.rowGroupsKept());
        // Page index
        assertEquals(false, snap.pageIndexUsed());
        assertEquals(0L, snap.rowsInKeptRowGroups());
        assertEquals(0L, snap.rowsAfterPageIndex());
        // Late mat
        assertEquals(false, snap.lateMaterializationEnabled());
        assertEquals(false, snap.lateMaterializationUsed());
        assertEquals(List.of(), snap.predicateColumns());
        // Aggregate
        assertEquals(0L, snap.readNanos());
        // No per-column entries recorded
        assertTrue(snap.columns().isEmpty());
    }

    public void testFooterCounters() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.addFooterRead(1234L, 65536L, 4);
        c.addFooterRead(56L, 0L, 0);

        var snap = c.snapshot();
        assertEquals(1290L, snap.footerReadNanos());
        assertEquals(65536L, snap.footerSizeBytes());
        assertEquals(4L, snap.rowGroupsInFile());
    }

    public void testFooterCacheHitMissCounters() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.recordFooterCache(false);
        c.recordFooterCache(true);
        c.recordFooterCache(true);
        c.recordFooterCache(true);

        var snap = c.snapshot();
        assertEquals(3L, snap.footerCacheHits());
        assertEquals(1L, snap.footerCacheMisses());
    }

    public void testRowGroupFilterCounters() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.addRowGroupFiltered(true);
        c.addRowGroupFiltered(false);
        c.addRowGroupFiltered(false);

        var snap = c.snapshot();
        assertEquals(3L, snap.rowGroupsTotal());
        assertEquals(1L, snap.rowGroupsKept());
    }

    public void testPageIndexCounters() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.markPageIndexUsed();
        c.addPageIndexRows(10_000L, 250L);

        var snap = c.snapshot();
        assertEquals(true, snap.pageIndexUsed());
        assertEquals(10_000L, snap.rowsInKeptRowGroups());
        assertEquals(250L, snap.rowsAfterPageIndex());
    }

    public void testLateMaterializationFlagsAndPredicateColumns() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.setLateMaterializationEnabled(true);
        c.markLateMaterializationUsed();
        c.addPredicateColumns(List.of("status_code", "host"));
        c.addPredicateColumns(List.of("host", "tenant_id"));

        var snap = c.snapshot();
        assertEquals(true, snap.lateMaterializationEnabled());
        assertEquals(true, snap.lateMaterializationUsed());
        List<String> predicates = snap.predicateColumns();
        // Sorted, deduplicated
        assertEquals(List.of("host", "status_code", "tenant_id"), List.copyOf(predicates));
    }

    public void testTotalReadNanos() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.addTotalReadNanos(100L);
        c.addTotalReadNanos(50L);
        // Negative / zero ignored
        c.addTotalReadNanos(0L);
        c.addTotalReadNanos(-5L);

        var snap = c.snapshot();
        assertEquals(150L, snap.readNanos());
    }

    public void testPerColumnSnapshotShape() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.perColumn("host").setMaterialization(PerColumnStatus.MATERIALIZATION_LATE);
        c.perColumn("status_code").setMaterialization(PerColumnStatus.MATERIALIZATION_EAGER);

        var snap = c.snapshot();
        Map<String, PerColumnStatus> columns = snap.columns();
        assertNotNull("per-column snapshot must be present when at least one column was touched", columns);
        assertEquals(2, columns.size());

        assertEquals(PerColumnStatus.MATERIALIZATION_LATE, columns.get("host").materialization());
        assertEquals(PerColumnStatus.MATERIALIZATION_EAGER, columns.get("status_code").materialization());
    }

    public void testPerColumnComputeIfAbsentReturnsSameInstance() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        ParquetReaderCounters.PerColumnCounters first = c.perColumn("host");
        ParquetReaderCounters.PerColumnCounters second = c.perColumn("host");
        assertSame("perColumn must return the same counter instance for the same key", first, second);
    }

    public void testCountersIncrementUnderConcurrency() throws Exception {
        ParquetReaderCounters counters = new ParquetReaderCounters();

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
                        counters.addFooterRead(13L, 1L, 0);
                        counters.addRowGroupFiltered(true);
                        counters.addTotalReadNanos(7L);
                        counters.perColumn("host").setMaterialization(PerColumnStatus.MATERIALIZATION_LATE);
                        if (i % 5 == 0) {
                            counters.addPredicateColumns(List.of("host"));
                        }
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

        long expectedIters = (long) threads * iterationsPerThread;
        var snap = counters.snapshot();
        assertEquals(expectedIters * 13L, snap.footerReadNanos());
        assertEquals(expectedIters, snap.footerSizeBytes());
        assertEquals(expectedIters, snap.rowGroupsTotal());
        assertEquals(expectedIters, snap.rowGroupsKept());
        assertEquals(expectedIters * 7L, snap.readNanos());
        Map<String, PerColumnStatus> columns = snap.columns();
        assertEquals(PerColumnStatus.MATERIALIZATION_LATE, columns.get("host").materialization());
        List<String> predicates = snap.predicateColumns();
        assertEquals(List.of("host"), predicates);
    }

    public void testSnapshotIsImmutable() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.addFooterRead(1L, 1L, 1);
        c.perColumn("host").setMaterialization(PerColumnStatus.MATERIALIZATION_EAGER);
        c.addPredicateColumns(List.of("host"));
        var snap = c.snapshot();
        // The collections exposed by the snapshot reject mutation.
        expectThrows(UnsupportedOperationException.class, () -> snap.columns().put("x", null));
        expectThrows(UnsupportedOperationException.class, () -> snap.predicateColumns().add("x"));
    }
}
