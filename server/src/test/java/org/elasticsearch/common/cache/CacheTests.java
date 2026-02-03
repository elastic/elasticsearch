/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToLongBiFunction;

import static org.elasticsearch.common.cache.RemovalNotification.RemovalReason.EVICTED;
import static org.elasticsearch.common.cache.RemovalNotification.RemovalReason.INVALIDATED;

public class CacheTests extends ESTestCase {

    private List<CacheSupplier<String, String>> knownImplementations;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        knownImplementations = List.of((removalListener, weigher) -> {
            CacheBuilder<String, String> builder = CacheBuilder.builder();
            weigher.ifPresent(builder::weigher);
            removalListener.ifPresent(builder::removalListener);
            return builder.build();
        });
        logger.debug("Testing known implementations: {}", knownImplementations);
    }

    @FunctionalInterface
    interface CacheSupplier<Key, Value> {
        default Cache<Key, Value> supply() {
            return supply(Optional.empty(), Optional.empty());
        }

        default Cache<Key, Value> supply(RemovalListener<Key, Value> removalListener) {
            return supply(Optional.ofNullable(removalListener), Optional.empty());
        }

        default Cache<Key, Value> supply(ToLongBiFunction<Key, Value> weigher) {
            return supply(Optional.empty(), Optional.ofNullable(weigher));
        }

        default Cache<Key, Value> supply(RemovalListener<Key, Value> removalListener, ToLongBiFunction<Key, Value> weigher) {
            return supply(Optional.ofNullable(removalListener), Optional.ofNullable(weigher));
        }

        Cache<Key, Value> supply(Optional<RemovalListener<Key, Value>> removalListener, Optional<ToLongBiFunction<Key, Value>> weigher);
    }

    /**
     * Tests the put and get functionality across all known implementations of the Cache.
     */
    public void testPutAndGet() {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            Cache<String, String> cache = supplier.supply();
            String key = randomAlphanumericOfLength(10);
            String value = randomAlphanumericOfLength(50);

            cache.put(key, value);
            assertEquals(value, cache.get(key));
        }
    }

    public void testCount() {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            Cache<String, String> cache = supplier.supply();

            int expectedCount = randomIntBetween(3, 10);
            for (int i = 0; i < expectedCount; i++) {
                cache.put(randomAlphaOfLength(10), randomAlphaOfLength(50));
            }
            assertEquals(expectedCount, cache.count());
        }
    }

    public void testWeight() {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            ToLongBiFunction<String, String> weigher = (key, value) -> value.length();
            Cache<String, String> cache = supplier.supply(weigher);

            int weight1 = randomIntBetween(10, 50);
            cache.put(randomAlphaOfLength(10), randomAlphaOfLength(weight1));
            int weight2 = randomIntBetween(10, 50);
            cache.put(randomAlphaOfLength(10), randomAlphaOfLength(weight2));
            assertEquals(weight1 + weight2, cache.weight()); // 6 + 11
        }
    }

    public void testInvalidate() throws ExecutionException, InterruptedException, TimeoutException {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            String key = randomAlphaOfLength(10);
            String value = randomAlphaOfLength(50);
            CompletableFuture<Boolean> notified = new CompletableFuture<>();

            RemovalListener<String, String> removalListener = (notification) -> {
                assertEquals(key, notification.getKey());
                assertEquals(value, notification.getValue());
                assertEquals(RemovalNotification.RemovalReason.INVALIDATED, notification.getRemovalReason());
                notified.complete(true);
            };
            Cache<String, String> cache = supplier.supply(removalListener);

            cache.put(key, value);
            cache.invalidate(key);

            assertTrue(notified.get(3, TimeUnit.SECONDS));
        }
    }

    public void testInvalidateAll() throws InterruptedException {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            LongAdder notified = new LongAdder();
            RemovalListener<String, String> removalListener = (notification) -> {
                assertEquals(RemovalNotification.RemovalReason.INVALIDATED, notification.getRemovalReason());
                notified.increment();
            };
            Cache<String, String> cache = supplier.supply(removalListener);

            int numberOfEntries = randomIntBetween(10, 50);
            for (int i = 0; i < numberOfEntries; i++) {
                cache.put(randomAlphaOfLength(10), randomAlphaOfLength(50));
            }
            cache.invalidateAll();

            for (int tries = 1; tries <= 10; tries++) {
                if (notified.longValue() == numberOfEntries) {
                    break;
                }
                wait(100);
            }
            assertEquals(numberOfEntries, notified.longValue());
            assertEquals(0, cache.count());
        }
    }

    public void testStats() {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            Map<String, String> entries = new HashMap<>();
            for (int i = 0; i < randomIntBetween(25, 100); i++) {
                entries.put(randomAlphaOfLength(10), randomAlphaOfLength(50));
            }

            Cache<String, String> cache = supplier.supply();
            entries.forEach(cache::put);

            int expectedHits = 0;
            int expectedMisses = 0;
            for (int i = 0; i < randomIntBetween(25, 100); i++) {
                String key = randomAlphaOfLength(10);
                if (entries.containsKey(key)) {
                    expectedHits++;
                    assertEquals(entries.get(key), cache.get(key));
                } else {
                    expectedMisses++;
                    assertNull(cache.get(key));
                }
            }

            int invalidations = randomIntBetween(10, entries.size() - 1);
            for (int i = 0; i < invalidations; i++) {
                String entry = randomFrom(entries.keySet());
                entries.remove(entry);
                cache.invalidate(entry);
            }

            Cache.Stats stats = cache.stats();
            assertEquals(expectedHits, stats.getHits());
            assertEquals(expectedMisses, stats.getMisses());
            assertEquals(invalidations, stats.getEvictions());
        }
    }

    public void testKeys() {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            Map<String, String> entries = new HashMap<>();
            for (int i = 0; i < randomIntBetween(25, 100); i++) {
                entries.put(randomAlphaOfLength(10), randomAlphaOfLength(11));
            }

            Cache<String, String> cache = supplier.supply();
            entries.forEach(cache::put);

            Set<String> expectedKeys = new HashSet<>(entries.keySet());
            cache.keys().forEach(key -> assertTrue(expectedKeys.remove(key)));
            assertTrue(expectedKeys.isEmpty());
        }
    }

    public void testKeysIterator() {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            Map<String, String> entries = new HashMap<>();
            for (int i = 0; i < randomIntBetween(25, 100); i++) {
                entries.put(randomAlphaOfLength(10), randomAlphaOfLength(11));
            }

            LongAdder notificationsReceived = new LongAdder();
            Cache<String, String> cache = supplier.supply((notification) -> {
                assertEquals(notification.getRemovalReason(), INVALIDATED);
                notificationsReceived.increment();
            });
            entries.forEach(cache::put);

            {
                Set<String> expectedKeys = new HashSet<>(entries.keySet());
                Set<String> removedKeys = new HashSet<>();
                Iterator<String> iterator = cache.keys().iterator();
                while (iterator.hasNext()) {
                    String entry = iterator.next();
                    assertTrue(expectedKeys.remove(entry));
                    if (randomBoolean()) {
                        iterator.remove();
                        removedKeys.add(entry);
                    }
                }
                assertTrue(expectedKeys.isEmpty());
                assertEquals(cache.count(), entries.size() - removedKeys.size());
                assertEquals(notificationsReceived.longValue(), removedKeys.size());
                removedKeys.forEach(entries::remove);
            }

            {
                Set<String> expectedKeys = new HashSet<>(entries.keySet());
                cache.keys().forEach(key -> assertTrue(expectedKeys.remove(key)));
                assertTrue(expectedKeys.isEmpty());
            }
        }
    }

    public void testValues() {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            Map<String, String> entries = new HashMap<>();
            for (int i = 0; i < randomIntBetween(25, 100); i++) {
                entries.put(randomAlphaOfLength(10), randomAlphaOfLength(11));
            }

            Cache<String, String> cache = supplier.supply();
            entries.forEach(cache::put);

            Collection<String> expectedValues = new ArrayList<>(entries.values());
            cache.values().forEach(key -> assertTrue(expectedValues.remove(key)));
            assertTrue(expectedValues.isEmpty());
        }
    }

    public void testValuesIterator() {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            Map<String, String> entries = new HashMap<>();
            for (int i = 0; i < randomIntBetween(25, 100); i++) {
                entries.put(randomAlphaOfLength(10), randomAlphaOfLength(11));
            }

            LongAdder notificationsReceived = new LongAdder();
            Cache<String, String> cache = supplier.supply((notification) -> {
                assertEquals(notification.getRemovalReason(), INVALIDATED);
                notificationsReceived.increment();
            });
            entries.forEach(cache::put);

            { // checking Iterator#remove()
                Collection<String> expectedValues = new ArrayList<>(entries.values());
                Collection<String> removedValues = new ArrayList<>();
                Iterator<String> iterator = cache.values().iterator();
                while (iterator.hasNext()) {
                    String entry = iterator.next();
                    assertTrue(expectedValues.remove(entry));
                    if (randomBoolean()) {
                        iterator.remove();
                        removedValues.add(entry);
                    }
                }
                assertTrue(expectedValues.isEmpty());
                assertEquals(cache.count(), entries.size() - removedValues.size());
                assertEquals(notificationsReceived.longValue(), removedValues.size());
                // multiple entries with the same value might cross but result should be eventually consistent, assuming cache emits
                // values in the same way as the hashmap we use here does, like if a Map impl. does de-dup + reference. Academic argument
                // I suppose, moving on...
                removedValues.forEach(value -> entries.values().remove(value));
            }

            { // checking if a new Iterable is consistent after the modifications
                Collection<String> expectedValues = new ArrayList<>(entries.values());
                cache.values().forEach(key -> assertTrue(expectedValues.remove(key)));
                assertTrue(expectedValues.isEmpty());
            }
        }
    }

    public void testForEach() {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            Map<String, String> entries = new HashMap<>();
            for (int i = 0; i < randomIntBetween(25, 100); i++) {
                entries.put(randomAlphaOfLength(10), randomAlphaOfLength(50));
            }

            Cache<String, String> cache = supplier.supply();
            entries.forEach(cache::put);

            Map<String, String> expectedEntries = new HashMap<>(entries);
            cache.forEach((key, value) -> assertTrue(expectedEntries.remove(key, value)));
            assertTrue(expectedEntries.isEmpty());
        }
    }

    public void testRefresh() {
        for (CacheSupplier<String, String> supplier : knownImplementations) {
            Map<String, String> entries = new HashMap<>();
            for (int i = 0; i < randomIntBetween(25, 100); i++) {
                entries.put(randomAlphaOfLength(10), randomAlphaOfLength(50));
            }

            Cache<String, String> cache = supplier.supply(notification -> {
                assertEquals(notification.getRemovalReason(), EVICTED);
                logger.debug("Eviction happened for key [{}], value [{}]", notification.getKey(), notification.getValue());
            },
                (key, value) -> Long.MAX_VALUE // maximize chance of triggering Eviction.
            );
            entries.forEach(cache::put);

            logger.debug("Calling refresh");
            cache.refresh();
        }
    }
}
