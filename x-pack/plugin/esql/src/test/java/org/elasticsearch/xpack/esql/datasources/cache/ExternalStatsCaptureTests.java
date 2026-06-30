/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ExternalStatsCaptureTests extends ESTestCase {

    public void testConcurrentRecordCapturesEveryFragment() throws Exception {
        // Intra-query hazard: parallel-parsing workers harvest stripes of the SAME file into one shared
        // sink concurrently. The ConcurrentMap (outer) + synchronizedList (inner) must capture every
        // fragment with none lost and no ConcurrentModificationException, regardless of interleaving.
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        String path = "file:///data/f.csv";
        int threads = 16;
        int perThread = 64;

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                exec.submit(() -> {
                    try (ExternalStatsCapture.Handle h = ExternalStatsCapture.bind(sink)) {
                        start.await();
                        for (int k = 0; k < perThread; k++) {
                            ExternalStatsCapture.record(path, Map.of("_stats.row_count", (long) (tid * perThread + k)));
                        }
                    } catch (Throwable th) {
                        failure.compareAndSet(null, th);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue("records did not finish", done.await(30, TimeUnit.SECONDS));
        } finally {
            exec.shutdown();
        }
        assertNull("record threw under contention: " + failure.get(), failure.get());
        assertEquals("every fragment captured, none lost", threads * perThread, sink.get(path).size());
    }

    public void testRecordWithoutBoundSinkIsNoOp() {
        // No sink bound on this thread → record is a silent no-op (no NPE), per the contract.
        ExternalStatsCapture.record("file:///x.csv", Map.of("_stats.row_count", 1L));
    }
}
