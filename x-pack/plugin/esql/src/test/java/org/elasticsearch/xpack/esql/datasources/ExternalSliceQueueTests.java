/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class ExternalSliceQueueTests extends ESTestCase {

    public void testEmptyQueue() {
        ExternalSliceQueue queue = new ExternalSliceQueue(List.of());
        assertEquals(0, queue.totalSlices());
        assertNull(queue.nextSplit());
        assertNull(queue.nextSplit());
    }

    public void testSingleSplit() {
        FileSplit split = fileSplit("s3://bucket/file1.parquet");
        ExternalSliceQueue queue = new ExternalSliceQueue(List.of(split));
        assertEquals(1, queue.totalSlices());
        assertSame(split, queue.nextSplit());
        assertNull(queue.nextSplit());
    }

    public void testMultipleSplits() {
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            splits.add(fileSplit("s3://bucket/file" + i + ".parquet"));
        }
        ExternalSliceQueue queue = new ExternalSliceQueue(splits);
        assertEquals(5, queue.totalSlices());

        Set<ExternalSplit> claimed = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < 5; i++) {
            ExternalSplit s = queue.nextSplit();
            assertNotNull(s);
            assertTrue("each split claimed exactly once", claimed.add(s));
        }
        assertNull(queue.nextSplit());
        assertEquals(5, claimed.size());
    }

    public void testExhaustionReturnsNull() {
        ExternalSliceQueue queue = new ExternalSliceQueue(List.of(fileSplit("s3://bucket/a.parquet")));
        assertNotNull(queue.nextSplit());
        for (int i = 0; i < 10; i++) {
            assertNull(queue.nextSplit());
        }
    }

    public void testNullSplitsThrows() {
        expectThrows(IllegalArgumentException.class, () -> new ExternalSliceQueue(null));
    }

    public void testConcurrentAccess() throws Exception {
        int splitCount = randomIntBetween(10, 100);
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(fileSplit("s3://bucket/file" + i + ".parquet"));
        }
        ExternalSliceQueue queue = new ExternalSliceQueue(splits);

        int threadCount = randomIntBetween(2, 8);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);
        Set<ExternalSplit> claimed = ConcurrentHashMap.newKeySet();
        AtomicInteger nullCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    while (true) {
                        ExternalSplit s = queue.nextSplit();
                        if (s == null) {
                            nullCount.incrementAndGet();
                            break;
                        }
                        assertTrue("duplicate split claimed", claimed.add(s));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }).start();
        }

        done.await();
        assertEquals(splitCount, claimed.size());
        assertEquals(threadCount, nullCount.get());
    }

    public void testConcurrentAccessAllSplitsClaimed() throws Exception {
        int splitCount = 50;
        List<ExternalSplit> splits = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            splits.add(fileSplit("s3://bucket/concurrent" + i + ".parquet"));
        }
        ExternalSliceQueue queue = new ExternalSliceQueue(splits);

        int threadCount = 10;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);
        List<List<ExternalSplit>> perThread = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            perThread.add(Collections.synchronizedList(new ArrayList<>()));
        }

        for (int t = 0; t < threadCount; t++) {
            final int threadIdx = t;
            new Thread(() -> {
                try {
                    barrier.await();
                    ExternalSplit s;
                    while ((s = queue.nextSplit()) != null) {
                        perThread.get(threadIdx).add(s);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }).start();
        }

        done.await();
        int totalClaimed = perThread.stream().mapToInt(List::size).sum();
        assertEquals(splitCount, totalClaimed);
    }

    private static FileSplit fileSplit(String path) {
        return new FileSplit("test", StoragePath.of(path), 0, 1024, "parquet", Map.of(), Map.of());
    }
}
