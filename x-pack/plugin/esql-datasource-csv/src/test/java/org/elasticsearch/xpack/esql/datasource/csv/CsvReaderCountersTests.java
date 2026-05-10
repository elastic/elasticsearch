/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CsvReaderCountersTests extends ESTestCase {

    public void testEmptySnapshotIsZeroes() {
        CsvReaderCounters counters = new CsvReaderCounters();
        Map<String, Object> snap = counters.snapshot();
        assertEquals(0L, snap.get("lines_read"));
        assertEquals(0L, snap.get("parse_errors"));
        assertEquals(false, snap.get("header_detected"));
        assertEquals(0L, snap.get("total_read_nanos"));
    }

    public void testSnapshotReflectsIncrements() {
        CsvReaderCounters counters = new CsvReaderCounters();
        counters.addLinesRead(10);
        counters.addParseErrors(3);
        counters.markHeaderDetected();
        counters.addReadNanos(987654);
        Map<String, Object> snap = counters.snapshot();
        assertEquals(10L, snap.get("lines_read"));
        assertEquals(3L, snap.get("parse_errors"));
        assertEquals(true, snap.get("header_detected"));
        assertEquals(987654L, snap.get("total_read_nanos"));
    }

    public void testNonPositiveDeltasIgnored() {
        CsvReaderCounters counters = new CsvReaderCounters();
        counters.addLinesRead(0);
        counters.addLinesRead(-5);
        counters.addParseErrors(0);
        counters.addParseErrors(-1);
        counters.addReadNanos(0);
        counters.addReadNanos(-1);
        Map<String, Object> snap = counters.snapshot();
        assertEquals(0L, snap.get("lines_read"));
        assertEquals(0L, snap.get("parse_errors"));
        assertEquals(0L, snap.get("total_read_nanos"));
    }

    public void testMarkHeaderDetectedIsMonotonic() {
        CsvReaderCounters counters = new CsvReaderCounters();
        counters.markHeaderDetected();
        counters.markHeaderDetected();
        counters.markHeaderDetected();
        Map<String, Object> snap = counters.snapshot();
        assertEquals(true, snap.get("header_detected"));
    }

    public void testConcurrentIncrementsAccumulateWithoutLoss() throws Exception {
        CsvReaderCounters counters = new CsvReaderCounters();
        int threads = 8;
        int iterationsPerThread = 1_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        counters.addLinesRead(3);
                        counters.addParseErrors(1);
                        counters.addReadNanos(40);
                        if (tid == 0 && i == 0) {
                            counters.markHeaderDetected();
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

        long expectedLines = (long) threads * iterationsPerThread * 3;
        long expectedErrors = (long) threads * iterationsPerThread;
        long expectedNanos = (long) threads * iterationsPerThread * 40;
        Map<String, Object> snap = counters.snapshot();
        assertEquals(expectedLines, snap.get("lines_read"));
        assertEquals(expectedErrors, snap.get("parse_errors"));
        assertEquals(expectedNanos, snap.get("total_read_nanos"));
        assertEquals(true, snap.get("header_detected"));
    }

    public void testSnapshotIsImmutable() {
        CsvReaderCounters counters = new CsvReaderCounters();
        counters.addLinesRead(5);
        Map<String, Object> snap = counters.snapshot();
        counters.addLinesRead(10);
        assertEquals(5L, snap.get("lines_read"));
        expectThrows(UnsupportedOperationException.class, () -> snap.put("lines_read", 0L));
    }
}
