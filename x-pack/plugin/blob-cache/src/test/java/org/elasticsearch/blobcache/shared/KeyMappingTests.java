/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;

public class KeyMappingTests extends ESTestCase {

    public void testBasics() {
        final String k1 = randomAlphanumericOfLength(10);
        final String k2 = randomAlphanumericOfLength(10);
        final String value = randomAlphanumericOfLength(10);
        KeyMapping<String, String, String> mapping = new KeyMapping<>();
        assertNull(mapping.get(k1, k2));

        assertEquals(value, mapping.computeIfAbsent(k1, k2, (kx) -> value));
        assertEquals(value, mapping.get(k1, k2));

        mapping.computeIfAbsent(k1, k2, (kx) -> { throw new AssertionError(); });

        assertEquals(value, mapping.get(k1, k2));

        final String k12 = randomValueOtherThan(k1, () -> randomAlphanumericOfLength(10));
        mapping.computeIfAbsent(k12, k2, (kx) -> randomAlphanumericOfLength(10));

        assertEquals(value, mapping.get(k1, k2));

        assertEquals(Set.of(k1, k12), mapping.key1s());

        Set<String> values = new HashSet<>();
        mapping.forEach(k1, (ak2, result) -> { assertTrue(values.add(result)); });
        assertEquals(Set.of(value), values);

        assertTrue(mapping.remove(k1, k2, value));

        assertEquals(Set.of(k12), mapping.key1s());

        assertNull(mapping.get(k1, k2));
        assertNotNull(mapping.get(k12, k2));

        assertFalse(mapping.remove(k1, k2, value));
    }

    public void testCountMatchingKey2s() {
        final var mapping = new KeyMapping<String, String, String>();

        // empty mapping always returns 0
        assertThat(mapping.countMatchingKey2s(k2 -> true), is(0L));

        // populate entries spread across multiple key1 values, with key2 values prefixed "match-" or "skip-"
        final var matching = randomLongBetween(3, 30);
        final var nonMatching = randomLongBetween(3, 30);
        for (var i = 0; i < matching; i++) {
            final var k1 = "k1-" + randomIntBetween(0, 4);
            mapping.computeIfAbsent(k1, "match-" + i, kx -> randomAlphanumericOfLength(5));
        }
        for (var i = 0; i < nonMatching; i++) {
            final var k1 = "k1-" + randomIntBetween(0, 4);
            mapping.computeIfAbsent(k1, "skip-" + i, kx -> randomAlphanumericOfLength(5));
        }

        assertThat(mapping.countMatchingKey2s(k2 -> k2.startsWith("match-")), is(matching));
        assertThat(mapping.countMatchingKey2s(k2 -> k2.startsWith("skip-")), is(nonMatching));
        assertThat(mapping.countMatchingKey2s(k2 -> true), is(matching + nonMatching));
        assertThat(mapping.countMatchingKey2s(k2 -> false), is(0L));

        // remove one matching entry — count must drop by exactly 1
        final var removedValue = mapping.get("k1-0", "match-0");
        if (removedValue != null) {
            assertThat(mapping.remove("k1-0", "match-0", removedValue), is(true));
            assertThat(mapping.countMatchingKey2s(k2 -> k2.startsWith("match-")), is(matching - 1));
        }
    }

    public void testMultiThreaded() {
        final String k1 = randomAlphanumericOfLength(10);
        KeyMapping<String, String, Integer> mapping = new KeyMapping<>();

        List<Thread> threads = IntStream.range(0, 10).mapToObj(i -> new Thread(() -> {
            final String k2 = Integer.toString(i);
            logger.info(k2);

            for (int j = 0; j < 1000; ++j) {
                Integer finalJ = j;
                assertNull(mapping.get(k1, k2));
                assertSame(finalJ, mapping.computeIfAbsent(k1, k2, (kx) -> finalJ));
                assertEquals(finalJ, mapping.get(k1, k2));
                assertTrue(mapping.remove(k1, k2, finalJ));
                if ((j & 1) == 0) {
                    assertFalse(mapping.remove(k1, k2, finalJ));
                }

            }
            assertNull(mapping.get(k1, k2));
        }, "test-thread-" + i)).toList();

        threads.forEach(Thread::start);
        threads.forEach(t -> {
            try {
                t.join(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(Set.of(), mapping.key1s());
    }

    public void testMultiThreadedSameKey() {
        final String k1 = randomAlphanumericOfLength(10);
        KeyMapping<String, String, Integer> mapping = new KeyMapping<>();

        List<Thread> threads = IntStream.range(0, 10).mapToObj(i -> new Thread(() -> {
            for (int j = 0; j < 1000; ++j) {
                Integer computeValue = i * 1000 + j;
                Integer value = mapping.computeIfAbsent(k1, k1, (kx) -> computeValue);
                assertNotNull(value);
                // either our value or another threads value.
                assertTrue(value == computeValue || value / 1000 != i);
                mapping.remove(k1, k1, value);
            }
        })).toList();
        threads.forEach(Thread::start);
        threads.forEach(t -> {
            try {
                t.join(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(Set.of(), mapping.key1s());
    }
}
