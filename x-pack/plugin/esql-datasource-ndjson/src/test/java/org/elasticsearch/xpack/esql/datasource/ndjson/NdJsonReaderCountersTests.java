/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NdJsonReaderCountersTests extends ESTestCase {

    public void testEmptySnapshotIsZeroes() {
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
        Map<String, Object> snap = counters.snapshot();
        assertEquals(0L, snap.get("documents_parsed"));
        assertEquals(0L, snap.get("parse_errors"));
        assertEquals(0L, snap.get("total_read_nanos"));
    }

    public void testSnapshotReflectsIncrements() {
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
        counters.addDocumentsParsed(7);
        counters.addParseErrors(2);
        counters.addReadNanos(123456);
        Map<String, Object> snap = counters.snapshot();
        assertEquals(7L, snap.get("documents_parsed"));
        assertEquals(2L, snap.get("parse_errors"));
        assertEquals(123456L, snap.get("total_read_nanos"));
    }

    public void testNonPositiveDeltasIgnored() {
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
        counters.addDocumentsParsed(0);
        counters.addDocumentsParsed(-3);
        counters.addParseErrors(0);
        counters.addParseErrors(-1);
        counters.addReadNanos(0);
        counters.addReadNanos(-1);
        Map<String, Object> snap = counters.snapshot();
        assertEquals(0L, snap.get("documents_parsed"));
        assertEquals(0L, snap.get("parse_errors"));
        assertEquals(0L, snap.get("total_read_nanos"));
    }

    public void testConcurrentIncrementsAccumulateWithoutLoss() throws Exception {
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
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
                        counters.addDocumentsParsed(2);
                        counters.addParseErrors(1);
                        counters.addReadNanos(50);
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

        long expectedDocs = (long) threads * iterationsPerThread * 2;
        long expectedErrors = (long) threads * iterationsPerThread;
        long expectedNanos = (long) threads * iterationsPerThread * 50;
        Map<String, Object> snap = counters.snapshot();
        assertEquals(expectedDocs, snap.get("documents_parsed"));
        assertEquals(expectedErrors, snap.get("parse_errors"));
        assertEquals(expectedNanos, snap.get("total_read_nanos"));
    }

    public void testSnapshotIsImmutableCopy() {
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
        counters.addDocumentsParsed(5);
        Map<String, Object> snap = counters.snapshot();
        // Mutating the underlying counters does not retroactively mutate a previously taken snapshot.
        counters.addDocumentsParsed(10);
        assertEquals(5L, snap.get("documents_parsed"));
        // The map itself rejects mutation.
        expectThrows(UnsupportedOperationException.class, () -> snap.put("documents_parsed", 0L));
    }
}
