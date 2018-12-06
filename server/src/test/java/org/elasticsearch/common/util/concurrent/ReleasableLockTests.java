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

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReleasableLockTests extends ESTestCase {

    /**
     * Test that accounting on whether or not a thread holds a releasable lock is correct. Previously we had a bug where on a re-entrant
     * lock that if a thread entered the lock twice we would declare that it does not hold the lock after it exits its first entrance but
     * not its second entrance.
     *
     * @throws BrokenBarrierException if awaiting on the synchronization barrier breaks
     * @throws InterruptedException   if awaiting on the synchronization barrier is interrupted
     */
    public void testIsHeldByCurrentThread() throws BrokenBarrierException, InterruptedException {
        final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        final ReleasableLock readLock = new ReleasableLock(readWriteLock.readLock());
        final ReleasableLock writeLock = new ReleasableLock(readWriteLock.writeLock());

        final int numberOfThreads = scaledRandomIntBetween(1, 32);
        final int iterations = scaledRandomIntBetween(1, 32);
        final CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; i++) {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                for (int j = 0; j < iterations; j++) {
                    if (randomBoolean()) {
                        acquire(readLock, writeLock);
                    } else {
                        acquire(writeLock, readLock);
                    }
                }
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            threads.add(thread);
            thread.start();
        }

        barrier.await();
        barrier.await();
        for (final Thread thread : threads) {
            thread.join();
        }
    }

    private void acquire(final ReleasableLock lockToAcquire, final ReleasableLock otherLock) {
        try (@SuppressWarnings("unused") Releasable outer = lockToAcquire.acquire()) {
            assertTrue(lockToAcquire.isHeldByCurrentThread());
            assertFalse(otherLock.isHeldByCurrentThread());
            try (@SuppressWarnings("unused") Releasable inner = lockToAcquire.acquire()) {
                assertTrue(lockToAcquire.isHeldByCurrentThread());
                assertFalse(otherLock.isHeldByCurrentThread());
            }
            // previously there was a bug here and this would return false
            assertTrue(lockToAcquire.isHeldByCurrentThread());
            assertFalse(otherLock.isHeldByCurrentThread());
        }
        assertFalse(lockToAcquire.isHeldByCurrentThread());
        assertFalse(otherLock.isHeldByCurrentThread());
    }

}
