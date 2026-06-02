/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NdJsonReaderCountersTests extends ESTestCase {

    public void testEmptySnapshotIsZeroes() {
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
        var snap = counters.snapshot();
        assertEquals("ndjson", snap.format());
        assertEquals(0L, snap.parseErrors());
        assertEquals(0L, snap.readNanos());
    }

    public void testSnapshotReflectsIncrements() {
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
        counters.addParseErrors(2);
        counters.addReadNanos(123456);
        var snap = counters.snapshot();
        assertEquals("ndjson", snap.format());
        assertEquals(2L, snap.parseErrors());
        assertEquals(123456L, snap.readNanos());
    }

    public void testNonPositiveDeltasIgnored() {
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
        counters.addParseErrors(0);
        counters.addParseErrors(-1);
        counters.addReadNanos(0);
        counters.addReadNanos(-1);
        var snap = counters.snapshot();
        assertEquals(0L, snap.parseErrors());
        assertEquals(0L, snap.readNanos());
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

        long expectedErrors = (long) threads * iterationsPerThread;
        long expectedNanos = (long) threads * iterationsPerThread * 50;
        var snap = counters.snapshot();
        assertEquals(expectedErrors, snap.parseErrors());
        assertEquals(expectedNanos, snap.readNanos());
    }

    public void testSnapshotIsImmutableCopy() {
        NdJsonReaderCounters counters = new NdJsonReaderCounters();
        counters.addParseErrors(5);
        var snap = counters.snapshot();
        // Mutating the underlying counters does not retroactively mutate a previously taken snapshot.
        counters.addParseErrors(10);
        assertEquals(5L, snap.parseErrors());
    }
}
