/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class KeyedLockTests extends ESTestCase {
    public void testIfMapEmptyAfterLotsOfAcquireAndReleases() throws InterruptedException {
        ConcurrentHashMap<String, Integer> counter = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, AtomicInteger> safeCounter = new ConcurrentHashMap<>();
        KeyedLock<String> connectionLock = new KeyedLock<>();
        String[] names = new String[randomIntBetween(1, 40)];
        for (int i = 0; i < names.length; i++) {
            names[i] = randomRealisticUnicodeOfLengthBetween(10, 20);
        }
        int numThreads = randomIntBetween(3, 10);
        final CountDownLatch startLatch = new CountDownLatch(1 + numThreads);
        AcquireAndReleaseThread[] threads = new AcquireAndReleaseThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new AcquireAndReleaseThread(startLatch, connectionLock, names, counter, safeCounter);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        startLatch.countDown();

        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
        assertThat(connectionLock.hasLockedKeys(), equalTo(false));

        Set<Entry<String, Integer>> entrySet = counter.entrySet();
        assertThat(counter.size(), equalTo(safeCounter.size()));
        for (Entry<String, Integer> entry : entrySet) {
            AtomicInteger atomicInteger = safeCounter.get(entry.getKey());
            assertThat(atomicInteger, not(Matchers.nullValue()));
            assertThat(atomicInteger.get(), equalTo(entry.getValue()));
        }
    }

    public void testHasLockedKeys() {
        KeyedLock<String> lock = new KeyedLock<>();
        assertFalse(lock.hasLockedKeys());
        Releasable foo = lock.acquire("foo");
        assertTrue(lock.hasLockedKeys());
        foo.close();
        assertFalse(lock.hasLockedKeys());
    }

    public void testTryAcquire() throws InterruptedException {
        KeyedLock<String> lock = new KeyedLock<>();
        Releasable foo = lock.tryAcquire("foo");
        Releasable second = lock.tryAcquire("foo");
        assertTrue(lock.hasLockedKeys());
        foo.close();
        assertTrue(lock.hasLockedKeys());
        second.close();
        assertFalse(lock.hasLockedKeys());
        // lock again
        Releasable acquire = lock.tryAcquire("foo");
        assertNotNull(acquire);
        final AtomicBoolean check = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            latch.countDown();
            try (Releasable ignore = lock.acquire("foo")) {
                assertTrue(check.get());
            }
        });
        thread.start();
        latch.await();
        check.set(true);
        acquire.close();
        foo.close();
        thread.join();
    }

    public void testLockIsReentrant() throws InterruptedException {
        KeyedLock<String> lock = new KeyedLock<>();
        Releasable foo = lock.acquire("foo");
        assertTrue(lock.isHeldByCurrentThread("foo"));
        assertFalse(lock.isHeldByCurrentThread("bar"));
        Releasable foo2 = lock.acquire("foo");
        AtomicInteger test = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            latch.countDown();
            try (Releasable r = lock.acquire("foo")) {
                test.incrementAndGet();
            }

        });
        t.start();
        latch.await();
        Thread.yield();
        assertEquals(0, test.get());
        List<Releasable> list = Arrays.asList(foo, foo2);
        Collections.shuffle(list, random());
        list.get(0).close();
        Thread.yield();
        assertEquals(0, test.get());
        list.get(1).close();
        t.join();
        assertEquals(1, test.get());
        assertFalse(lock.hasLockedKeys());
    }

    public static class AcquireAndReleaseThread extends Thread {
        private CountDownLatch startLatch;
        KeyedLock<String> connectionLock;
        String[] names;
        ConcurrentHashMap<String, Integer> counter;
        ConcurrentHashMap<String, AtomicInteger> safeCounter;
        final int numRuns = scaledRandomIntBetween(5000, 50000);

        public AcquireAndReleaseThread(
            CountDownLatch startLatch,
            KeyedLock<String> connectionLock,
            String[] names,
            ConcurrentHashMap<String, Integer> counter,
            ConcurrentHashMap<String, AtomicInteger> safeCounter
        ) {
            this.startLatch = startLatch;
            this.connectionLock = connectionLock;
            this.names = names;
            this.counter = counter;
            this.safeCounter = safeCounter;
        }

        @Override
        public void run() {
            startLatch.countDown();
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < numRuns; i++) {
                String curName = names[randomInt(names.length - 1)];
                assert connectionLock.isHeldByCurrentThread(curName) == false;
                Releasable lock;
                if (randomIntBetween(0, 10) < 4) {
                    int tries = 0;
                    boolean stepOut = false;
                    while ((lock = connectionLock.tryAcquire(curName)) == null) {
                        assertFalse(connectionLock.isHeldByCurrentThread(curName));
                        if (tries++ == 10) {
                            stepOut = true;
                            break;
                        }
                    }
                    if (stepOut) {
                        break;
                    }
                } else {
                    lock = connectionLock.acquire(curName);
                }
                try (Releasable ignore = lock) {
                    assert connectionLock.isHeldByCurrentThread(curName);
                    assert connectionLock.isHeldByCurrentThread(curName + "bla") == false;
                    if (randomBoolean()) {
                        try (Releasable reentrantIgnored = connectionLock.acquire(curName)) {
                            // just acquire this and make sure we can :)
                            Thread.yield();
                        }
                    }
                    Integer integer = counter.get(curName);
                    if (integer == null) {
                        counter.put(curName, 1);
                    } else {
                        counter.put(curName, integer.intValue() + 1);
                    }
                }
                AtomicInteger atomicInteger = new AtomicInteger(0);
                AtomicInteger value = safeCounter.putIfAbsent(curName, atomicInteger);
                if (value == null) {
                    atomicInteger.incrementAndGet();
                } else {
                    value.incrementAndGet();
                }
            }
        }
    }
}
