/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class SizeBlockingQueueTests extends ESTestCase {

    /*
     * Tests that the size of a queue remains at most the capacity while offers are made to a queue when at capacity. This test would have
     * previously failed when the size of the queue was incremented and exposed externally even though the item offered to the queue was
     * never actually added to the queue.
     */
    public void testQueueSize() throws InterruptedException {
        final int capacity = randomIntBetween(1, 32);
        final BlockingQueue<Integer> blockingQueue = new ArrayBlockingQueue<>(capacity);
        final SizeBlockingQueue<Integer> sizeBlockingQueue = new SizeBlockingQueue<>(blockingQueue, capacity);

        // fill the queue to capacity
        for (int i = 0; i < capacity; i++) {
            sizeBlockingQueue.offer(i);
        }


        final int iterations = 1 << 16;
        final CyclicBarrier barrier = new CyclicBarrier(2);

        // this thread will try to offer items to the queue while the queue size thread is polling the size
        final Thread queueOfferThread = new Thread(() -> {
            for (int i = 0; i < iterations; i++) {
                try {
                    // synchronize each iteration of checking the size with each iteration of offering, each iteration is a race
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                sizeBlockingQueue.offer(capacity + i);
            }
        });
        queueOfferThread.start();

        // this thread will repeatedly poll the size of the queue keeping track of the maximum size that it sees
        final AtomicInteger maxSize = new AtomicInteger();
        final Thread queueSizeThread = new Thread(() -> {
            for (int i = 0; i < iterations; i++) {
                try {
                    // synchronize each iteration of checking the size with each iteration of offering, each iteration is a race
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                maxSize.set(Math.max(maxSize.get(), sizeBlockingQueue.size()));
            }
        });
        queueSizeThread.start();

        // wait for the threads to finish
        queueOfferThread.join();
        queueSizeThread.join();

        // the maximum size of the queue should be equal to the capacity
        assertThat(maxSize.get(), equalTo(capacity));
    }

}
