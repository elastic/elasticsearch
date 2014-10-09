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

package org.elasticsearch.transport.netty;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class KeyedLockTests extends ElasticsearchTestCase {

    @Test
    public void checkIfMapEmptyAfterLotsOfAcquireAndReleases() throws InterruptedException {
        ConcurrentHashMap<String, Integer> counter = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, AtomicInteger> safeCounter = new ConcurrentHashMap<>();
        KeyedLock<String> connectionLock = randomBoolean() ? new KeyedLock.GlobalLockable<String>() : new KeyedLock<String>();
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

        Set<Entry<String, Integer>> entrySet = counter.entrySet();
        assertThat(counter.size(), equalTo(safeCounter.size()));
        for (Entry<String, Integer> entry : entrySet) {
            AtomicInteger atomicInteger = safeCounter.get(entry.getKey());
            assertThat(atomicInteger, not(Matchers.nullValue()));
            assertThat(atomicInteger.get(), equalTo(entry.getValue()));
        }
    }

    @Test(expected = ElasticsearchIllegalStateException.class)
    public void checkCannotAcquireTwoLocksGlobal() throws InterruptedException {
        KeyedLock.GlobalLockable<String> connectionLock = new KeyedLock.GlobalLockable<>();
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
        KeyedLock<String> connectionLock = randomBoolean() ? new KeyedLock.GlobalLockable<String>() : new KeyedLock<String>();
        String name = randomRealisticUnicodeOfLength(scaledRandomIntBetween(10, 50));
        connectionLock.acquire(name);
        connectionLock.acquire(name);
    }

    @Test(expected = ElasticsearchIllegalStateException.class)
    public void checkCannotReleaseUnacquiredLock() throws InterruptedException {
        KeyedLock<String> connectionLock = randomBoolean() ? new KeyedLock.GlobalLockable<String>() : new KeyedLock<String>();
        String name = randomRealisticUnicodeOfLength(scaledRandomIntBetween(10, 50));
        connectionLock.release(name);
    }

    public static class AcquireAndReleaseThread extends Thread {
        private CountDownLatch startLatch;
        KeyedLock<String> connectionLock;
        String[] names;
        ConcurrentHashMap<String, Integer> counter;
        ConcurrentHashMap<String, AtomicInteger> safeCounter;

        public AcquireAndReleaseThread(CountDownLatch startLatch, KeyedLock<String> connectionLock, String[] names,
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
            if (connectionLock instanceof KeyedLock.GlobalLockable) {
                final int iters = randomIntBetween(10, 200);
                for (int i = 0; i < iters; i++) {
                    ((KeyedLock.GlobalLockable) connectionLock).globalLock().lock();
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
                        ((KeyedLock.GlobalLockable) connectionLock).globalLock().unlock();
                    }
                }
            }
        }
    }
}
