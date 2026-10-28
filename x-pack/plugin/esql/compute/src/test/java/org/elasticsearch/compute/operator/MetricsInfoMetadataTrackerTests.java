/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class MetricsInfoMetadataTrackerTests extends ESTestCase {
    public void testEachMetricMustObserveItsOwnDimensions() {
        MetricsInfoMetadataTracker tracker = new MetricsInfoMetadataTracker(
            Map.of(
                "metrics",
                List.of(
                    new MetricFieldInfo("cpu", "index", "double", "gauge", null),
                    new MetricFieldInfo("disk", "index", "long", "counter", "bytes")
                )
            ),
            Map.of("metrics", Set.of("host", "region"))
        );
        tracker.observe("cpu", "metrics", Set.of(), Set.of("double"), Set.of("gauge"), Set.of("host", "region"));
        tracker.observe("disk", "metrics", Set.of("bytes"), Set.of("long"), Set.of("counter"), Set.of("host"));
        assertFalse(tracker.isComplete());
        tracker.observe("disk", "metrics", Set.of("bytes"), Set.of("long"), Set.of("counter"), Set.of("host", "region"));
        assertTrue(tracker.isComplete());
    }

    public void testUnexpectedMetadataDoesNotComplete() {
        MetricsInfoMetadataTracker tracker = new MetricsInfoMetadataTracker(
            Map.of("metrics", List.of(new MetricFieldInfo("cpu", "index", "double", "gauge", null))),
            Map.of("metrics", Set.of("host"))
        );
        tracker.observe("cpu", "metrics", Set.of(), Set.of("double"), Set.of("gauge"), Set.of("host", "extra"));
        assertFalse(tracker.isComplete());
    }

    public void testConcurrentDuplicateObservations() throws InterruptedException {
        MetricsInfoMetadataTracker tracker = new MetricsInfoMetadataTracker(
            Map.of(
                "first",
                List.of(new MetricFieldInfo("cpu", "index-a", "double", "gauge", null)),
                "second",
                List.of(new MetricFieldInfo("cpu", "index-b", "double", "gauge", null))
            ),
            Map.of("first", Set.of("host"), "second", Set.of("host"))
        );
        CountDownLatch start = new CountDownLatch(1);
        Thread[] threads = new Thread[between(2, 8)];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                for (int repeat = 0; repeat < 100; repeat++) {
                    for (String dataStream : List.of("first", "second")) {
                        tracker.observe("cpu", dataStream, Set.of(), Set.of("double"), Set.of("gauge"), Set.of("host"));
                    }
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertTrue(tracker.isComplete());
    }
}
