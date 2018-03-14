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

package org.elasticsearch.index.engine;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageTester;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Assertions;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

public class LiveVersionMapTests extends ESTestCase {

    public void testRamBytesUsed() throws Exception {
        assumeTrue("Test disabled for JDK 9", JavaVersion.current().compareTo(JavaVersion.parse("9")) < 0);
        LiveVersionMap map = new LiveVersionMap();
        for (int i = 0; i < 100000; ++i) {
            BytesRefBuilder uid = new BytesRefBuilder();
            uid.copyChars(TestUtil.randomSimpleString(random(), 10, 20));
            VersionValue version = new VersionValue(randomLong(), randomLong(), randomLong());
            try (Releasable r = map.acquireLock(uid.toBytesRef())) {
                map.putUnderLock(uid.toBytesRef(), version);
            }
        }
        long actualRamBytesUsed = RamUsageTester.sizeOf(map);
        long estimatedRamBytesUsed = map.ramBytesUsed();
        // less than 50% off
        assertEquals(actualRamBytesUsed, estimatedRamBytesUsed, actualRamBytesUsed / 2);

        // now refresh
        map.beforeRefresh();
        map.afterRefresh(true);

        for (int i = 0; i < 100000; ++i) {
            BytesRefBuilder uid = new BytesRefBuilder();
            uid.copyChars(TestUtil.randomSimpleString(random(), 10, 20));
            VersionValue version = new VersionValue(randomLong(), randomLong(), randomLong());
            try (Releasable r = map.acquireLock(uid.toBytesRef())) {
                map.putUnderLock(uid.toBytesRef(), version);
            }
        }
        actualRamBytesUsed = RamUsageTester.sizeOf(map);
        estimatedRamBytesUsed = map.ramBytesUsed();
        // less than 25% off
        assertEquals(actualRamBytesUsed, estimatedRamBytesUsed, actualRamBytesUsed / 4);
    }

    private BytesRef uid(String string) {
        BytesRefBuilder builder = new BytesRefBuilder();
        builder.copyChars(string);
        // length of the array must be the same as the len of the ref... there is an assertion in LiveVersionMap#putUnderLock
        return BytesRef.deepCopyOf(builder.get());
    }

    public void testBasics() throws IOException {
        LiveVersionMap map = new LiveVersionMap();
        try (Releasable r = map.acquireLock(uid("test"))) {
            map.putUnderLock(uid("test"), new VersionValue(1,1,1));
            assertEquals(new VersionValue(1,1,1), map.getUnderLock(uid("test")));
            map.beforeRefresh();
            assertEquals(new VersionValue(1,1,1), map.getUnderLock(uid("test")));
            map.afterRefresh(randomBoolean());
            assertNull(map.getUnderLock(uid("test")));


            map.putUnderLock(uid("test"), new DeleteVersionValue(1,1,1,1));
            assertEquals(new DeleteVersionValue(1,1,1,1), map.getUnderLock(uid("test")));
            map.beforeRefresh();
            assertEquals(new DeleteVersionValue(1,1,1,1), map.getUnderLock(uid("test")));
            map.afterRefresh(randomBoolean());
            assertEquals(new DeleteVersionValue(1,1,1,1), map.getUnderLock(uid("test")));
            map.pruneTombstones(2, 0);
            assertNull(map.getUnderLock(uid("test")));
        }
    }

    public void testConcurrently() throws IOException, InterruptedException {
        HashSet<BytesRef> keySet = new HashSet<>();
        int numKeys = randomIntBetween(50, 200);
        for (int i = 0; i < numKeys; i++) {
            keySet.add(uid(TestUtil.randomSimpleString(random(), 10, 20)));
        }
        List<BytesRef> keyList = new ArrayList<>(keySet);
        ConcurrentHashMap<BytesRef, VersionValue> values = new ConcurrentHashMap<>();
        ConcurrentHashMap<BytesRef, DeleteVersionValue> deletes = new ConcurrentHashMap<>();
        LiveVersionMap map = new LiveVersionMap();
        int numThreads = randomIntBetween(2, 5);

        Thread[] threads = new Thread[numThreads];
        CountDownLatch startGun = new CountDownLatch(numThreads);
        CountDownLatch done = new CountDownLatch(numThreads);
        int randomValuesPerThread = randomIntBetween(5000, 20000);
        AtomicLong clock = new AtomicLong(0);
        AtomicLong lastPrunedTimestamp = new AtomicLong(-1);
        for (int j = 0; j < threads.length; j++) {
            threads[j] = new Thread(() -> {
                startGun.countDown();
                try {
                    startGun.await();
                } catch (InterruptedException e) {
                    done.countDown();
                    throw new AssertionError(e);
                }
                try {
                    for (int i = 0; i < randomValuesPerThread; ++i) {
                        BytesRef bytesRef = randomFrom(random(), keyList);
                        final long clockTick = clock.get();
                        try (Releasable r = map.acquireLock(bytesRef)) {
                            VersionValue versionValue = values.computeIfAbsent(bytesRef,
                                v -> new VersionValue(randomLong(), randomLong(), randomLong()));
                            boolean isDelete = versionValue instanceof DeleteVersionValue;
                            if (isDelete) {
                                map.removeTombstoneUnderLock(bytesRef);
                                deletes.remove(bytesRef);
                            }
                            if (isDelete == false && rarely()) {
                                versionValue = new DeleteVersionValue(versionValue.version + 1, versionValue.seqNo + 1,
                                    versionValue.term, clock.getAndIncrement());
                                deletes.put(bytesRef, (DeleteVersionValue) versionValue);
                            } else {
                                versionValue = new VersionValue(versionValue.version + 1, versionValue.seqNo + 1, versionValue.term);
                            }
                            values.put(bytesRef, versionValue);
                            map.putUnderLock(bytesRef, versionValue);
                        }
                        if (rarely()) {
                            map.pruneTombstones(clockTick, 0);
                            // timestamp we pruned the deletes
                            lastPrunedTimestamp.updateAndGet(prev -> Math.max(clockTick, prev)); // make sure we track the latest
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
            threads[j].start();
        }
        do {
            final Map<BytesRef, VersionValue> valueMap = new HashMap<>(map.getAllCurrent());
            map.beforeRefresh();
            valueMap.forEach((k, v) -> {
                try (Releasable r = map.acquireLock(k)) {
                    VersionValue actualValue = map.getUnderLock(k);
                    assertNotNull(actualValue);
                    assertTrue(v.version <= actualValue.version);
                }
            });
            map.afterRefresh(randomBoolean());
            valueMap.forEach((k, v) -> {
                try (Releasable r = map.acquireLock(k)) {
                    VersionValue actualValue = map.getUnderLock(k);
                    if (actualValue != null) {
                        if (actualValue instanceof DeleteVersionValue) {
                            assertTrue(v.version <= actualValue.version); // deletes can be the same version
                        } else {
                            assertTrue(v.version < actualValue.version);
                        }

                    }
                }
            });
            if (randomBoolean()) {
                Thread.yield();
            }
        } while (done.getCount() != 0);

        for (int j = 0; j < threads.length; j++) {
            threads[j].join();
        }
        map.getAllCurrent().forEach((k, v) -> {
            VersionValue versionValue = values.get(k);
            assertNotNull(versionValue);
            assertEquals(v, versionValue);
        });
        Runnable assertTombstones = () ->
            map.getAllTombstones().entrySet().forEach(e -> {
                VersionValue versionValue = values.get(e.getKey());
                assertNotNull(versionValue);
                assertEquals(e.getValue(), versionValue);
                assertTrue(versionValue instanceof DeleteVersionValue);
            });
        assertTombstones.run();
        map.beforeRefresh();
        assertTombstones.run();
        map.afterRefresh(false);
        assertTombstones.run();

        deletes.entrySet().forEach(e -> {
            try (Releasable r = map.acquireLock(e.getKey())) {
                VersionValue value = map.getUnderLock(e.getKey());
                // here we keep track of the deletes and ensure that all deletes that are not visible anymore ie. not in the map
                // have a timestamp that is smaller or equal to the maximum timestamp that we pruned on
                if (value == null) {
                    assertTrue(e.getValue().time + " > " + lastPrunedTimestamp.get(), e.getValue().time <= lastPrunedTimestamp.get());
                } else {
                    assertEquals(value, e.getValue());
                }
            }
        });
        map.pruneTombstones(clock.incrementAndGet(), 0);
        assertEquals(0, StreamSupport.stream(map.getAllTombstones().entrySet().spliterator(), false).count());
    }

    public void testCarryOnSafeAccess() throws IOException {
        LiveVersionMap map = new LiveVersionMap();
        assertFalse(map.isUnsafe());
        assertFalse(map.isSafeAccessRequired());
        map.enforceSafeAccess();
        assertTrue(map.isSafeAccessRequired());
        assertFalse(map.isUnsafe());
        int numIters = randomIntBetween(1, 5);
        for (int i = 0; i < numIters; i++) { // if we don't do anything ie. no adds etc we will stay with the safe access required
            map.beforeRefresh();
            map.afterRefresh(randomBoolean());
            assertTrue("failed in iter: " + i, map.isSafeAccessRequired());
        }

        try (Releasable r = map.acquireLock(uid(""))) {
            map.maybePutUnderLock(new BytesRef(""), new VersionValue(randomLong(), randomLong(), randomLong()));
        }
        assertFalse(map.isUnsafe());
        assertEquals(1, map.getAllCurrent().size());

        map.beforeRefresh();
        map.afterRefresh(randomBoolean());
        assertFalse(map.isUnsafe());
        assertFalse(map.isSafeAccessRequired());
        try (Releasable r = map.acquireLock(uid(""))) {
            map.maybePutUnderLock(new BytesRef(""), new VersionValue(randomLong(), randomLong(), randomLong()));
        }
        assertTrue(map.isUnsafe());
        assertFalse(map.isSafeAccessRequired());
        assertEquals(0, map.getAllCurrent().size());
    }

    public void testRefreshTransition() throws IOException {
        LiveVersionMap map = new LiveVersionMap();
        try (Releasable r = map.acquireLock(uid("1"))) {
            map.maybePutUnderLock(uid("1"), new VersionValue(randomLong(), randomLong(), randomLong()));
            assertTrue(map.isUnsafe());
            assertNull(map.getUnderLock(uid("1")));
            map.beforeRefresh();
            assertTrue(map.isUnsafe());
            assertNull(map.getUnderLock(uid("1")));
            map.afterRefresh(randomBoolean());
            assertNull(map.getUnderLock(uid("1")));
            assertFalse(map.isUnsafe());

            map.enforceSafeAccess();
            map.maybePutUnderLock(uid("1"), new VersionValue(randomLong(), randomLong(), randomLong()));
            assertFalse(map.isUnsafe());
            assertNotNull(map.getUnderLock(uid("1")));
            map.beforeRefresh();
            assertFalse(map.isUnsafe());
            assertTrue(map.isSafeAccessRequired());
            assertNotNull(map.getUnderLock(uid("1")));
            map.afterRefresh(randomBoolean());
            assertNull(map.getUnderLock(uid("1")));
            assertFalse(map.isUnsafe());
            assertTrue(map.isSafeAccessRequired());
        }
    }

    public void testAddAndDeleteRefreshConcurrently() throws IOException, InterruptedException {
        LiveVersionMap map = new LiveVersionMap();
        int numIters = randomIntBetween(1000, 5000);
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicLong version = new AtomicLong();
        CountDownLatch start = new CountDownLatch(2);
        BytesRef uid = uid("1");
        VersionValue initialVersion = new VersionValue(version.incrementAndGet(), 1, 1);
        try (Releasable ignore = map.acquireLock(uid)) {
            map.putUnderLock(uid, initialVersion);
        }
        Thread t = new Thread(() -> {
            start.countDown();
            try {
                start.await();
                VersionValue nextVersionValue = initialVersion;
                for (int i = 0; i < numIters; i++) {
                    try (Releasable ignore = map.acquireLock(uid)) {
                        VersionValue underLock = map.getUnderLock(uid);
                        if (underLock != null) {
                            assertEquals(underLock, nextVersionValue);
                        } else {
                            underLock = nextVersionValue;
                        }
                        if (underLock.isDelete()) {
                            nextVersionValue = new VersionValue(version.incrementAndGet(), 1, 1);
                        } else if (randomBoolean()) {
                            nextVersionValue = new VersionValue(version.incrementAndGet(), 1, 1);
                        } else {
                            nextVersionValue = new DeleteVersionValue(version.incrementAndGet(), 1, 1, 0);
                        }
                        map.putUnderLock(uid, nextVersionValue);
                    }
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                done.set(true);
            }
        });
        t.start();
        start.countDown();
        while(done.get() == false) {
            map.beforeRefresh();
            Thread.yield();
            map.afterRefresh(false);
        }
        t.join();

        try (Releasable ignore = map.acquireLock(uid)) {
            VersionValue underLock = map.getUnderLock(uid);
            if (underLock != null) {
                assertEquals(version.get(), underLock.version);
            }
        }
    }

    public void testPruneTombstonesWhileLocked() throws InterruptedException, IOException {
        LiveVersionMap map = new LiveVersionMap();
        BytesRef uid = uid("1");
        ;
        try (Releasable ignore = map.acquireLock(uid)) {
            map.putUnderLock(uid, new DeleteVersionValue(0, 0, 0, 0));
            map.beforeRefresh(); // refresh otherwise we won't prune since it's tracked by the current map
            map.afterRefresh(false);
            Thread thread = new Thread(() -> {
                map.pruneTombstones(Long.MAX_VALUE, 0);
            });
            thread.start();
            thread.join();
            assertEquals(1, map.getAllTombstones().size());
        }
        Thread thread = new Thread(() -> {
            map.pruneTombstones(Long.MAX_VALUE, 0);
        });
        thread.start();
        thread.join();
        assertEquals(0, map.getAllTombstones().size());
    }
}
