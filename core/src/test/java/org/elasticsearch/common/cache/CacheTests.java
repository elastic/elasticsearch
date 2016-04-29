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

package org.elasticsearch.common.cache;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class CacheTests extends ESTestCase {
    private int numberOfEntries;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        numberOfEntries = randomIntBetween(1000, 10000);
        logger.debug("numberOfEntries: {}", numberOfEntries);
    }

    // cache some entries, then randomly lookup keys that do not exist, then check the stats
    public void testCacheStats() {
        AtomicLong evictions = new AtomicLong();
        Set<Integer> keys = new HashSet<>();
        Cache<Integer, String> cache =
                CacheBuilder.<Integer, String>builder()
                        .setMaximumWeight(numberOfEntries / 2)
                        .removalListener(notification -> {
                            keys.remove(notification.getKey());
                            evictions.incrementAndGet();
                        })
                        .build();

        for (int i = 0; i < numberOfEntries; i++) {
            // track the keys, which will be removed upon eviction (see the RemovalListener)
            keys.add(i);
            cache.put(i, Integer.toString(i));
        }
        long hits = 0;
        long misses = 0;
        Integer missingKey = 0;
        for (Integer key : keys) {
            --missingKey;
            if (rarely()) {
                misses++;
                cache.get(missingKey);
            } else {
                hits++;
                cache.get(key);
            }
        }
        assertEquals(hits, cache.stats().getHits());
        assertEquals(misses, cache.stats().getMisses());
        assertEquals((long) Math.ceil(numberOfEntries / 2.0), evictions.get());
        assertEquals(evictions.get(), cache.stats().getEvictions());
    }

    // cache some entries in batches of size maximumWeight; for each batch, touch the even entries to affect the
    // ordering; upon the next caching of entries, the entries from the previous batch will be evicted; we can then
    // check that the evicted entries were evicted in LRU order (first the odds in a batch, then the evens in a batch)
    // for each batch
    public void testCacheEvictions() {
        int maximumWeight = randomIntBetween(1, numberOfEntries);
        AtomicLong evictions = new AtomicLong();
        List<Integer> evictedKeys = new ArrayList<>();
        Cache<Integer, String> cache =
                CacheBuilder.<Integer, String>builder()
                        .setMaximumWeight(maximumWeight)
                        .removalListener(notification -> {
                            evictions.incrementAndGet();
                            evictedKeys.add(notification.getKey());
                        })
                        .build();
        // cache entries up to numberOfEntries - maximumWeight; all of these entries will ultimately be evicted in
        // batches of size maximumWeight, first the odds in the batch, then the evens in the batch
        List<Integer> expectedEvictions = new ArrayList<>();
        int iterations = (int)Math.ceil((numberOfEntries - maximumWeight) / (1.0 * maximumWeight));
        for (int i = 0; i < iterations; i++) {
            for (int j = i * maximumWeight; j < (i + 1) * maximumWeight && j < numberOfEntries - maximumWeight; j++) {
                cache.put(j, Integer.toString(j));
                if (j % 2 == 1) {
                    expectedEvictions.add(j);
                }
            }
            for (int j = i * maximumWeight; j < (i + 1) * maximumWeight && j < numberOfEntries - maximumWeight; j++) {
                if (j % 2 == 0) {
                    cache.get(j);
                    expectedEvictions.add(j);
                }
            }
        }
        // finish filling the cache
        for (int i = numberOfEntries - maximumWeight; i < numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        assertEquals(numberOfEntries - maximumWeight, evictions.get());
        assertEquals(evictions.get(), cache.stats().getEvictions());

        // assert that the keys were evicted in LRU order
        Set<Integer> keys = new HashSet<>();
        List<Integer> remainingKeys = new ArrayList<>();
        for (Integer key : cache.keys()) {
            keys.add(key);
            remainingKeys.add(key);
        }
        assertEquals(expectedEvictions.size(), evictedKeys.size());
        for (int i = 0; i < expectedEvictions.size(); i++) {
            assertFalse(keys.contains(expectedEvictions.get(i)));
            assertEquals(expectedEvictions.get(i), evictedKeys.get(i));
        }
        for (int i = numberOfEntries - maximumWeight; i < numberOfEntries; i++) {
            assertTrue(keys.contains(i));
            assertEquals(
                    numberOfEntries - i + (numberOfEntries - maximumWeight) - 1,
                    (int) remainingKeys.get(i - (numberOfEntries - maximumWeight))
            );
        }
    }

    // cache some entries and exceed the maximum weight, then check that the cache has the expected weight and the
    // expected evictions occurred
    public void testWeigher() {
        int maximumWeight = 2 * numberOfEntries;
        int weight = randomIntBetween(2, 10);
        AtomicLong evictions = new AtomicLong();
        Cache<Integer, String> cache =
                CacheBuilder.<Integer, String>builder()
                        .setMaximumWeight(maximumWeight)
                        .weigher((k, v) -> weight)
                        .removalListener(notification -> evictions.incrementAndGet())
                        .build();
        for (int i = 0; i < numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        // cache weight should be the largest multiple of weight less than maximumWeight
        assertEquals(weight * (maximumWeight / weight), cache.weight());

        // the number of evicted entries should be the number of entries that fit in the excess weight
        assertEquals((int) Math.ceil((weight - 2) * numberOfEntries / (1.0 * weight)), evictions.get());

        assertEquals(evictions.get(), cache.stats().getEvictions());
    }

    // cache some entries, randomly invalidate some of them, then check that the weight of the cache is correct
    public void testWeight() {
        Cache<Integer, String> cache =
                CacheBuilder.<Integer, String>builder()
                        .weigher((k, v) -> k)
                        .build();
        int weight = 0;
        for (int i = 0; i < numberOfEntries; i++) {
            weight += i;
            cache.put(i, Integer.toString(i));
        }
        for (int i = 0; i < numberOfEntries; i++) {
            if (rarely()) {
                weight -= i;
                cache.invalidate(i);
            }
        }
        assertEquals(weight, cache.weight());
    }

    // cache some entries, randomly invalidate some of them, then check that the number of cached entries is correct
    public void testCount() {
        Cache<Integer, String> cache = CacheBuilder.<Integer, String>builder().build();
        int count = 0;
        for (int i = 0; i < numberOfEntries; i++) {
            count++;
            cache.put(i, Integer.toString(i));
        }
        for (int i = 0; i < numberOfEntries; i++) {
            if (rarely()) {
                count--;
                cache.invalidate(i);
            }
        }
        assertEquals(count, cache.count());
    }

    // cache some entries, step the clock forward, cache some more entries, step the clock forward and then check that
    // the first batch of cached entries expired and were removed
    public void testExpirationAfterAccess() {
        AtomicLong now = new AtomicLong();
        Cache<Integer, String> cache = new Cache<Integer, String>() {
            @Override
            protected long now() {
                return now.get();
            }
        };
        cache.setExpireAfterAccess(1);
        List<Integer> evictedKeys = new ArrayList<>();
        cache.setRemovalListener(notification -> {
            assertEquals(RemovalNotification.RemovalReason.EVICTED, notification.getRemovalReason());
            evictedKeys.add(notification.getKey());
        });
        now.set(0);
        for (int i = 0; i < numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        now.set(1);
        for (int i = numberOfEntries; i < 2 * numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        now.set(2);
        cache.refresh();
        assertEquals(numberOfEntries, cache.count());
        for (int i = 0; i < evictedKeys.size(); i++) {
            assertEquals(i, (int) evictedKeys.get(i));
        }
        Set<Integer> remainingKeys = new HashSet<>();
        for (Integer key : cache.keys()) {
            remainingKeys.add(key);
        }
        for (int i = numberOfEntries; i < 2 * numberOfEntries; i++) {
            assertTrue(remainingKeys.contains(i));
        }
    }

    public void testExpirationAfterWrite() {
        AtomicLong now = new AtomicLong();
        Cache<Integer, String> cache = new Cache<Integer, String>() {
            @Override
            protected long now() {
                return now.get();
            }
        };
        cache.setExpireAfterWrite(1);
        List<Integer> evictedKeys = new ArrayList<>();
        cache.setRemovalListener(notification -> {
            assertEquals(RemovalNotification.RemovalReason.EVICTED, notification.getRemovalReason());
            evictedKeys.add(notification.getKey());
        });
        now.set(0);
        for (int i = 0; i < numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        now.set(1);
        for (int i = numberOfEntries; i < 2 * numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        now.set(2);
        for (int i = 0; i < numberOfEntries; i++) {
            cache.get(i);
        }
        cache.refresh();
        assertEquals(numberOfEntries, cache.count());
        for (int i = 0; i < evictedKeys.size(); i++) {
            assertEquals(i, (int) evictedKeys.get(i));
        }
        Set<Integer> remainingKeys = new HashSet<>();
        for (Integer key : cache.keys()) {
            remainingKeys.add(key);
        }
        for (int i = numberOfEntries; i < 2 * numberOfEntries; i++) {
            assertTrue(remainingKeys.contains(i));
        }
    }

    // randomly promote some entries, step the clock forward, then check that the promoted entries remain and the
    // non-promoted entries were removed
    public void testPromotion() {
        AtomicLong now = new AtomicLong();
        Cache<Integer, String> cache = new Cache<Integer, String>() {
            @Override
            protected long now() {
                return now.get();
            }
        };
        cache.setExpireAfterAccess(1);
        now.set(0);
        for (int i = 0; i < numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        now.set(1);
        Set<Integer> promotedKeys = new HashSet<>();
        for (int i = 0; i < numberOfEntries; i++) {
            if (rarely()) {
                cache.get(i);
                promotedKeys.add(i);
            }
        }
        now.set(2);
        cache.refresh();
        assertEquals(promotedKeys.size(), cache.count());
        for (int i = 0; i < numberOfEntries; i++) {
            if (promotedKeys.contains(i)) {
                assertNotNull(cache.get(i));
            } else {
                assertNull(cache.get(i));
            }
        }
    }


    // randomly invalidate some cached entries, then check that a lookup for each of those and only those keys is null
    public void testInvalidate() {
        Cache<Integer, String> cache = CacheBuilder.<Integer, String>builder().build();
        for (int i = 0; i < numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        Set<Integer> keys = new HashSet<>();
        for (Integer key : cache.keys()) {
            if (rarely()) {
                cache.invalidate(key);
                keys.add(key);
            }
        }
        for (int i = 0; i < numberOfEntries; i++) {
            if (keys.contains(i)) {
                assertNull(cache.get(i));
            } else {
                assertNotNull(cache.get(i));
            }
        }
    }

    // randomly invalidate some cached entries, then check that we receive invalidate notifications for those and only
    // those entries
    public void testNotificationOnInvalidate() {
        Set<Integer> notifications = new HashSet<>();
        Cache<Integer, String> cache =
                CacheBuilder.<Integer, String>builder()
                        .removalListener(notification -> {
                            assertEquals(RemovalNotification.RemovalReason.INVALIDATED, notification.getRemovalReason());
                            notifications.add(notification.getKey());
                        })
                        .build();
        for (int i = 0; i < numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        Set<Integer> invalidated = new HashSet<>();
        for (int i = 0; i < numberOfEntries; i++) {
            if (rarely()) {
                cache.invalidate(i);
                invalidated.add(i);
            }
        }
        assertEquals(notifications, invalidated);
    }

    // invalidate all cached entries, then check that the cache is empty
    public void testInvalidateAll() {
        Cache<Integer, String> cache = CacheBuilder.<Integer, String>builder().build();
        for (int i = 0; i < numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        cache.invalidateAll();
        assertEquals(0, cache.count());
        assertEquals(0, cache.weight());
    }

    // invalidate all cached entries, then check that we receive invalidate notifications for all entries
    public void testNotificationOnInvalidateAll() {
        Set<Integer> notifications = new HashSet<>();
        Cache<Integer, String> cache =
                CacheBuilder.<Integer, String>builder()
                        .removalListener(notification -> {
                            assertEquals(RemovalNotification.RemovalReason.INVALIDATED, notification.getRemovalReason());
                            notifications.add(notification.getKey());
                        })
                        .build();
        Set<Integer> invalidated = new HashSet<>();
        for (int i = 0; i < numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
            invalidated.add(i);
        }
        cache.invalidateAll();
        assertEquals(invalidated, notifications);
    }

    // randomly replace some entries, increasing the weight by 1 for each replacement, then count that the cache size
    // is correct
    public void testReplaceRecomputesSize() {
        class Value {
            private String value;
            private long weight;

            public Value(String value, long weight) {
                this.value = value;
                this.weight = weight;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Value that = (Value) o;

                return value.equals(that.value);
            }

            @Override
            public int hashCode() {
                return value.hashCode();
            }
        }
        Cache<Integer, Value> cache = CacheBuilder.<Integer, Value>builder().weigher((k, s) -> s.weight).build();
        for (int i = 0; i < numberOfEntries; i++) {
            cache.put(i, new Value(Integer.toString(i), 1));
        }
        assertEquals(numberOfEntries, cache.count());
        assertEquals(numberOfEntries, cache.weight());
        int replaced = 0;
        for (int i = 0; i < numberOfEntries; i++) {
            if (rarely()) {
                replaced++;
                cache.put(i, new Value(Integer.toString(i), 2));
            }
        }
        assertEquals(numberOfEntries, cache.count());
        assertEquals(numberOfEntries + replaced, cache.weight());
    }

    // randomly replace some entries, then check that we received replacement notifications for those and only those
    // entries
    public void testNotificationOnReplace() {
        Set<Integer> notifications = new HashSet<>();
        Cache<Integer, String> cache =
                CacheBuilder.<Integer, String>builder()
                        .removalListener(notification -> {
                            assertEquals(RemovalNotification.RemovalReason.REPLACED, notification.getRemovalReason());
                            notifications.add(notification.getKey());
                        })
                        .build();
        for (int i = 0; i < numberOfEntries; i++) {
            cache.put(i, Integer.toString(i));
        }
        Set<Integer> replacements = new HashSet<>();
        for (int i = 0; i < numberOfEntries; i++) {
            if (rarely()) {
                cache.put(i, Integer.toString(i) + Integer.toString(i));
                replacements.add(i);
            }
        }
        assertEquals(replacements, notifications);
    }

    public void testComputeIfAbsentLoadsSuccessfully() {
        Map<Integer, Integer> map = new HashMap<>();
        Cache<Integer, Integer> cache = CacheBuilder.<Integer, Integer>builder().build();
        for (int i = 0; i < numberOfEntries; i++) {
            try {
                cache.computeIfAbsent(i, k -> {
                    int value = randomInt();
                    map.put(k, value);
                    return value;
                });
            } catch (ExecutionException e) {
                throw new AssertionError(e);
            }
        }
        for (int i = 0; i < numberOfEntries; i++) {
            assertEquals(map.get(i), cache.get(i));
        }
    }

    public void testComputeIfAbsentCallsOnce() throws BrokenBarrierException, InterruptedException {
        int numberOfThreads = randomIntBetween(2, 32);
        final Cache<Integer, String> cache = CacheBuilder.<Integer, String>builder().build();
        AtomicReferenceArray flags = new AtomicReferenceArray(numberOfEntries);
        for (int j = 0; j < numberOfEntries; j++) {
            flags.set(j, false);
        }

        CopyOnWriteArrayList<ExecutionException> failures = new CopyOnWriteArrayList<>();

        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int j = 0; j < numberOfEntries; j++) {
                        try {
                            cache.computeIfAbsent(j, key -> {
                                assertTrue(flags.compareAndSet(key, false, true));
                                return Integer.toString(key);
                            });
                        } catch (ExecutionException e) {
                            failures.add(e);
                            break;
                        }
                    }
                    barrier.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
        }

        // wait for all threads to be ready
        barrier.await();
        // wait for all threads to finish
        barrier.await();

        assertThat(failures, is(empty()));
    }

    public void testComputeIfAbsentThrowsExceptionIfLoaderReturnsANullValue() {
        final Cache<Integer, String> cache = CacheBuilder.<Integer, String>builder().build();
        try {
            cache.computeIfAbsent(1, k -> null);
            fail("expected ExecutionException");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(NullPointerException.class));
        }
    }

    public void testDependentKeyDeadlock() throws BrokenBarrierException, InterruptedException {
        class Key {
            private final int key;

            public Key(int key) {
                this.key = key;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Key key1 = (Key) o;

                return key == key1.key;

            }

            @Override
            public int hashCode() {
                return key % 2;
            }
        }

        int numberOfThreads = randomIntBetween(2, 32);
        final Cache<Key, Integer> cache = CacheBuilder.<Key, Integer>builder().build();

        CopyOnWriteArrayList<ExecutionException> failures = new CopyOnWriteArrayList<>();

        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        CountDownLatch deadlockLatch = new CountDownLatch(numberOfThreads);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; i++) {
            Thread thread = new Thread(() -> {
                try {
                    try {
                        barrier.await();
                    } catch (BrokenBarrierException | InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    Random random = new Random(random().nextLong());
                    for (int j = 0; j < numberOfEntries; j++) {
                        Key key = new Key(random.nextInt(numberOfEntries));
                        try {
                            cache.computeIfAbsent(key, k -> {
                                if (k.key == 0) {
                                    return 0;
                                } else {
                                    Integer value = cache.get(new Key(k.key / 2));
                                    return value != null ? value : 0;
                                }
                            });
                        } catch (ExecutionException e) {
                            failures.add(e);
                            break;
                        }
                    }
                } finally {
                    // successfully avoided deadlock, release the main thread
                    deadlockLatch.countDown();
                }
            });
            threads.add(thread);
            thread.start();
        }

        AtomicBoolean deadlock = new AtomicBoolean();
        assert !deadlock.get();

        // start a watchdog service
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            Set<Long> ids = threads.stream().map(t -> t.getId()).collect(Collectors.toSet());
            ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
            long[] deadlockedThreads = mxBean.findDeadlockedThreads();
            if (!deadlock.get() && deadlockedThreads != null) {
                for (long deadlockedThread : deadlockedThreads) {
                    // ensure that we detected deadlock on our threads
                    if (ids.contains(deadlockedThread)) {
                        deadlock.set(true);
                        // release the main test thread to fail the test
                        for (int i = 0; i < numberOfThreads; i++) {
                            deadlockLatch.countDown();
                        }
                        break;
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // everything is setup, release the hounds
        barrier.await();

        // wait for either deadlock to be detected or the threads to terminate
        deadlockLatch.await();

        // shutdown the watchdog service
        scheduler.shutdown();

        assertThat(failures, is(empty()));

        assertFalse("deadlock", deadlock.get());
    }

    public void testCachePollution() throws BrokenBarrierException, InterruptedException {
        int numberOfThreads = randomIntBetween(2, 32);
        final Cache<Integer, String> cache = CacheBuilder.<Integer, String>builder().build();

        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    Random random = new Random(random().nextLong());
                    for (int j = 0; j < numberOfEntries; j++) {
                        Integer key = random.nextInt(numberOfEntries);
                        boolean first;
                        boolean second;
                        do {
                            first = random.nextBoolean();
                            second = random.nextBoolean();
                        } while (first && second);
                        if (first) {
                            try {
                                cache.computeIfAbsent(key, k -> {
                                    if (random.nextBoolean()) {
                                        return Integer.toString(k);
                                    } else {
                                        throw new Exception("testCachePollution");
                                    }
                                });
                            } catch (ExecutionException e) {
                                assertNotNull(e.getCause());
                                assertThat(e.getCause(), instanceOf(Exception.class));
                                assertEquals(e.getCause().getMessage(), "testCachePollution");
                            }
                        } else if (second) {
                            cache.invalidate(key);
                        } else {
                            cache.get(key);
                        }
                    }
                    barrier.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
        }

        // wait for all threads to be ready
        barrier.await();
        // wait for all threads to finish
        barrier.await();
    }

    // test that the cache is not corrupted under lots of concurrent modifications, even hitting the same key
    // here be dragons: this test did catch one subtle bug during development; do not remove lightly
    public void testTorture() throws BrokenBarrierException, InterruptedException {
        int numberOfThreads = randomIntBetween(2, 32);
        final Cache<Integer, String> cache =
                CacheBuilder.<Integer, String>builder()
                        .setMaximumWeight(1000)
                        .weigher((k, v) -> 2)
                        .build();

        CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    Random random = new Random(random().nextLong());
                    for (int j = 0; j < numberOfEntries; j++) {
                        Integer key = random.nextInt(numberOfEntries);
                        cache.put(key, Integer.toString(j));
                    }
                    barrier.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
        }

        // wait for all threads to be ready
        barrier.await();
        // wait for all threads to finish
        barrier.await();

        cache.refresh();
        assertEquals(500, cache.count());
    }
}
