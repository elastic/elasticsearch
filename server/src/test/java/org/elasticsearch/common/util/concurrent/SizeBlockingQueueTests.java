/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
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

        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicBoolean spin = new AtomicBoolean(true);
        final AtomicInteger maxSize = new AtomicInteger();

        // this thread will repeatedly poll the size of the queue keeping track of the maximum size that it sees
        final Thread queueSizeThread = new Thread(() -> {
            try {
                latch.await();
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
            while (spin.get()) {
                maxSize.set(Math.max(maxSize.get(), sizeBlockingQueue.size()));
            }
        });
        queueSizeThread.start();

        // this thread will try to offer items to the queue while the queue size thread is polling the size
        final Thread queueOfferThread = new Thread(() -> {
            try {
                latch.await();
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < 4096; i++) {
                sizeBlockingQueue.offer(capacity + i);
            }
        });
        queueOfferThread.start();

        // synchronize the start of the two threads
        latch.countDown();

        // wait for the offering thread to finish
        queueOfferThread.join();

        // stop the queue size thread
        spin.set(false);
        queueSizeThread.join();

        // the maximum size of the queue should be equal to the capacity
        assertThat(maxSize.get(), equalTo(capacity));
    }

}
