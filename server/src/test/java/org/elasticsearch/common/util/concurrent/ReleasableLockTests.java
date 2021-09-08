/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
        try (@SuppressWarnings("unused") Releasable outer = randomAcquireMethod(lockToAcquire)) {
            assertTrue(lockToAcquire.isHeldByCurrentThread());
            assertFalse(otherLock.isHeldByCurrentThread());
            try (@SuppressWarnings("unused") Releasable inner = randomAcquireMethod(lockToAcquire)) {
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

    private ReleasableLock randomAcquireMethod(ReleasableLock lock) {
        if (randomBoolean()) {
            return lock.acquire();
        } else {
            try {
                ReleasableLock releasableLock = lock.tryAcquire(TimeValue.timeValueSeconds(30));
                assertThat(releasableLock, notNullValue());
                return releasableLock;
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    public void testTryAcquire() throws Exception {
        ReleasableLock lock = new ReleasableLock(new ReentrantLock());
        int numberOfThreads = randomIntBetween(1, 10);
        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        AtomicInteger lockedCounter = new AtomicInteger();
        int timeout = randomFrom(0, 5, 10);
        List<Thread> threads =
            IntStream.range(0, numberOfThreads).mapToObj(i -> new Thread(() -> {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    try (ReleasableLock locked = lock.tryAcquire(TimeValue.timeValueMillis(timeout))) {
                        if (locked != null) {
                            lockedCounter.incrementAndGet();
                        }
                    }
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new AssertionError(e);
                }
            })).collect(Collectors.toList());
        threads.forEach(Thread::start);
        try (ReleasableLock locked = randomBoolean() ? lock.acquire() : null) {
            barrier.await(10, TimeUnit.SECONDS);
            for (Thread thread : threads) {
                thread.join(10000);
            }
            threads.forEach(t -> assertThat(t.isAlive(), is(false)));

            if (locked != null) {
                assertThat(lockedCounter.get(), equalTo(0));
            } else {
                assertThat(lockedCounter.get(), greaterThanOrEqualTo(1));
            }
        }

        try (ReleasableLock locked = lock.tryAcquire(TimeValue.ZERO)) {
            assertThat(locked, notNullValue());
        }
    }
}
