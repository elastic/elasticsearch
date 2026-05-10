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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ParquetReaderCountersTests extends ESTestCase {

    public void testEmptySnapshotShape() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        Map<String, Object> snap = c.snapshot();

        // Footer
        assertEquals(0L, snap.get("footer_read_nanos"));
        assertEquals(0L, snap.get("footer_size_bytes"));
        assertEquals(0L, snap.get("row_groups_in_file"));
        // Row-group filter
        assertEquals(0L, snap.get("row_groups_total"));
        assertEquals(0L, snap.get("row_groups_passed_stats"));
        assertEquals(0L, snap.get("row_groups_passed_dictionary"));
        assertEquals(0L, snap.get("row_groups_passed_bloom"));
        assertEquals(0L, snap.get("row_groups_kept"));
        // Page index
        assertEquals(false, snap.get("page_index_used"));
        assertEquals(0L, snap.get("rows_in_kept_row_groups"));
        assertEquals(0L, snap.get("rows_after_page_index"));
        // Late mat
        assertEquals(false, snap.get("late_materialization_enabled"));
        assertEquals(false, snap.get("late_materialization_used"));
        assertEquals(Set.of(), snap.get("predicate_columns"));
        // Aggregate
        assertEquals(0L, snap.get("read_nanos"));
        // Empty per-column map: omitted from snapshot
        assertFalse(snap.containsKey("columns"));
    }

    public void testFooterCounters() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.addFooterRead(1234L, 65536L, 4);
        c.addFooterRead(56L, 0L, 0);

        Map<String, Object> snap = c.snapshot();
        assertEquals(1290L, snap.get("footer_read_nanos"));
        assertEquals(65536L, snap.get("footer_size_bytes"));
        assertEquals(4L, snap.get("row_groups_in_file"));
    }

    public void testRowGroupFilterCounters() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.addRowGroupFiltered(true, true, true, true);
        c.addRowGroupFiltered(true, false, false, false);
        c.addRowGroupFiltered(false, false, false, false);

        Map<String, Object> snap = c.snapshot();
        assertEquals(3L, snap.get("row_groups_total"));
        assertEquals(2L, snap.get("row_groups_passed_stats"));
        assertEquals(1L, snap.get("row_groups_passed_dictionary"));
        assertEquals(1L, snap.get("row_groups_passed_bloom"));
        assertEquals(1L, snap.get("row_groups_kept"));
    }

    public void testPageIndexCounters() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.markPageIndexUsed();
        c.addPageIndexRows(10_000L, 250L);

        Map<String, Object> snap = c.snapshot();
        assertEquals(true, snap.get("page_index_used"));
        assertEquals(10_000L, snap.get("rows_in_kept_row_groups"));
        assertEquals(250L, snap.get("rows_after_page_index"));
    }

    public void testLateMaterializationFlagsAndPredicateColumns() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.setLateMaterializationEnabled(true);
        c.markLateMaterializationUsed();
        c.addPredicateColumns(List.of("status_code", "host"));
        c.addPredicateColumns(List.of("host", "tenant_id"));

        Map<String, Object> snap = c.snapshot();
        assertEquals(true, snap.get("late_materialization_enabled"));
        assertEquals(true, snap.get("late_materialization_used"));
        @SuppressWarnings("unchecked")
        Set<String> predicates = (Set<String>) snap.get("predicate_columns");
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

        Map<String, Object> snap = c.snapshot();
        assertEquals(150L, snap.get("read_nanos"));
    }

    public void testPerColumnSnapshotShape() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        ParquetReaderCounters.PerColumnCounters host = c.perColumn("host");
        host.addBytesCompressedRead(1024L);
        host.addBytesDecompressed(4096L);
        host.addDecompressionNanos(1000L);
        host.addDecodeNanos(500L);
        host.addPagesRead(3L);
        host.setMaterialization(PerColumnStatus.MATERIALIZATION_LATE);

        ParquetReaderCounters.PerColumnCounters status = c.perColumn("status_code");
        status.setMaterialization(PerColumnStatus.MATERIALIZATION_EAGER);

        Map<String, Object> snap = c.snapshot();
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> columns = (Map<String, Map<String, Object>>) snap.get("columns");
        assertNotNull("per-column snapshot must be present when at least one column was touched", columns);
        assertEquals(2, columns.size());

        Map<String, Object> hostSnap = columns.get("host");
        assertNotNull(hostSnap);
        assertEquals(1024L, hostSnap.get("compressed_bytes"));
        assertEquals(4096L, hostSnap.get("decompressed_bytes"));
        assertEquals(1000L, hostSnap.get("decompression_nanos"));
        assertEquals(500L, hostSnap.get("decode_nanos"));
        assertEquals(3L, hostSnap.get("data_pages_read"));
        assertEquals(PerColumnStatus.MATERIALIZATION_LATE, hostSnap.get("materialization"));

        Map<String, Object> statusSnap = columns.get("status_code");
        assertNotNull(statusSnap);
        assertEquals(PerColumnStatus.MATERIALIZATION_EAGER, statusSnap.get("materialization"));
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
                        counters.addRowGroupFiltered(true, true, true, true);
                        counters.addTotalReadNanos(7L);
                        counters.perColumn("host").addPagesRead(1L);
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
        Map<String, Object> snap = counters.snapshot();
        assertEquals(expectedIters * 13L, snap.get("footer_read_nanos"));
        assertEquals(expectedIters, snap.get("footer_size_bytes"));
        assertEquals(expectedIters, snap.get("row_groups_total"));
        assertEquals(expectedIters, snap.get("row_groups_kept"));
        assertEquals(expectedIters * 7L, snap.get("read_nanos"));
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> columns = (Map<String, Map<String, Object>>) snap.get("columns");
        assertEquals(expectedIters, columns.get("host").get("data_pages_read"));
        @SuppressWarnings("unchecked")
        Set<String> predicates = (Set<String>) snap.get("predicate_columns");
        assertEquals(Set.of("host"), predicates);
    }

    public void testSnapshotIsImmutable() {
        ParquetReaderCounters c = new ParquetReaderCounters();
        c.addFooterRead(1L, 1L, 1);
        Map<String, Object> snap = c.snapshot();
        expectThrows(UnsupportedOperationException.class, () -> snap.put("x", 1L));
    }
}
