/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.concurrent;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.netty.util.internal.ConcurrentHashMap;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 */
public class FairKeyedLockTests extends ElasticsearchTestCase{

    @Test
    public void checkIfMapEmptyAfterLotsOfAcquireAndReleases() throws InterruptedException {
        ConcurrentHashMap<String, Integer> counter = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, AtomicInteger> safeCounter = new ConcurrentHashMap<>();
        FairKeyedLock<String> connectionLock = randomBoolean() ? new FairKeyedLock.GlobalLockable<String>() : new FairKeyedLock<String>();
        String[] names = new String[randomIntBetween(1, 40)];
        for (int i = 0; i < names.length; i++) {
            names[i] = randomRealisticUnicodeOfLengthBetween(10, 20);
        }
        CountDownLatch startLatch = new CountDownLatch(1);
        int numThreads = randomIntBetween(3, 10);
        AcquireAndReleaseThread[] threads = new AcquireAndReleaseThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new AcquireAndReleaseThread(startLatch, connectionLock, names, counter, safeCounter);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        startLatch.countDown();
        for (int i = 0; i < numThreads; i++) {
            if (randomBoolean()) {
                threads[i].incWithGlobal();
            }
        }

        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
        assertThat(connectionLock.hasLockedKeys(), equalTo(false));

        Set<Map.Entry<String, Integer>> entrySet = counter.entrySet();
        assertThat(counter.size(), equalTo(safeCounter.size()));
        for (Map.Entry<String, Integer> entry : entrySet) {
            AtomicInteger atomicInteger = safeCounter.get(entry.getKey());
            assertThat(atomicInteger, not(Matchers.nullValue()));
            assertThat(atomicInteger.get(), equalTo(entry.getValue()));
        }
    }

    @Test(expected = ElasticsearchIllegalStateException.class)
    public void checkCannotAcquireTwoLocksGlobal() throws InterruptedException {
        FairKeyedLock.GlobalLockable<String> connectionLock = new FairKeyedLock.GlobalLockable<>();
        String name = randomRealisticUnicodeOfLength(scaledRandomIntBetween(10, 50));
        connectionLock.acquire(name);
        try {
            connectionLock.acquire(name);
        } finally {
            connectionLock.release(name);
            connectionLock.globalLock().lock();
            connectionLock.globalLock().unlock();
        }
    }

    @Test(expected = ElasticsearchIllegalStateException.class)
    public void checkCannotAcquireTwoLocks() throws InterruptedException {
        FairKeyedLock<String> connectionLock = randomBoolean() ? new FairKeyedLock.GlobalLockable<String>() : new FairKeyedLock<String>();
        String name = randomRealisticUnicodeOfLength(scaledRandomIntBetween(10, 50));
        connectionLock.acquire(name);
        connectionLock.acquire(name);
    }

    @Test(expected = ElasticsearchIllegalStateException.class)
    public void checkCannotReleaseUnacquiredLock() throws InterruptedException {
        FairKeyedLock<String> connectionLock = randomBoolean() ? new FairKeyedLock.GlobalLockable<String>() : new FairKeyedLock<String>();
        String name = randomRealisticUnicodeOfLength(scaledRandomIntBetween(10, 50));
        connectionLock.release(name);
    }

    public static class AcquireAndReleaseThread extends Thread {
        private CountDownLatch startLatch;
        FairKeyedLock<String> connectionLock;
        String[] names;
        ConcurrentHashMap<String, Integer> counter;
        ConcurrentHashMap<String, AtomicInteger> safeCounter;

        public AcquireAndReleaseThread(CountDownLatch startLatch, FairKeyedLock<String> connectionLock, String[] names,
                                       ConcurrentHashMap<String, Integer> counter, ConcurrentHashMap<String, AtomicInteger> safeCounter) {
            this.startLatch = startLatch;
            this.connectionLock = connectionLock;
            this.names = names;
            this.counter = counter;
            this.safeCounter = safeCounter;
        }

        public void run() {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
            int numRuns = scaledRandomIntBetween(5000, 50000);
            for (int i = 0; i < numRuns; i++) {
                String curName = names[randomInt(names.length - 1)];
                connectionLock.acquire(curName);
                try {
                    Integer integer = counter.get(curName);
                    if (integer == null) {
                        counter.put(curName, 1);
                    } else {
                        counter.put(curName, integer.intValue() + 1);
                    }
                } finally {
                    connectionLock.release(curName);
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

        public void incWithGlobal() {
            if (connectionLock instanceof FairKeyedLock.GlobalLockable) {
                final int iters = randomIntBetween(10, 200);
                for (int i = 0; i < iters; i++) {
                    ((FairKeyedLock.GlobalLockable) connectionLock).globalLock().lock();
                    try {
                        String curName = names[randomInt(names.length - 1)];
                        Integer integer = counter.get(curName);
                        if (integer == null) {
                            counter.put(curName, 1);
                        } else {
                            counter.put(curName, integer.intValue() + 1);
                        }
                        AtomicInteger atomicInteger = new AtomicInteger(0);
                        AtomicInteger value = safeCounter.putIfAbsent(curName, atomicInteger);
                        if (value == null) {
                            atomicInteger.incrementAndGet();
                        } else {
                            value.incrementAndGet();
                        }
                    } finally {
                        ((FairKeyedLock.GlobalLockable) connectionLock).globalLock().unlock();
                    }
                }
            }
        }
    }

}
