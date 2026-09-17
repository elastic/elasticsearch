/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class Int3HashTests extends ESTestCase {
    private BigArrays randombigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private Int3Hash randomHash() {
        // Test high load factors to make sure that collision resolution works fine
        final float maxLoadFactor = 0.6f + randomFloat() * 0.39f;
        return new Int3Hash(randomIntBetween(0, 100), maxLoadFactor, randombigArrays());
    }

    public void testAddDiscardsKeyRefusedByCircuitBreaker() {
        // set() claims the slot before append() stores the keys, and append() grows the key array, so a refusal
        // there leaves a slot pointing at an id whose keys were never written.
        CrankyCircuitBreakerService breakerService = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService, true);
        // Only the adds that grow allocate, so a single pass can see no trip at all, and here the damage is
        // usually silent as well: a slot left claiming an id whose keys were never written still answers lookups
        // correctly once a later key takes that id. Many short passes are needed to land an observable one.
        for (int attempt = 0; attempt < 1500; attempt++) {
            List<Integer> expected = new ArrayList<>();
            try (Int3Hash hash = new Int3Hash(1, bigArrays)) {
                for (int key = 0; key < 300; key++) {
                    try {
                        assertThat(hash.add(key, key + 1, key + 2), equalTo((long) expected.size()));
                        expected.add(key);
                    } catch (CircuitBreakingException e) {
                        // the key must be rejected outright, leaving the hash exactly as it was
                        assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
                    }
                }
                assertThat(hash.size(), equalTo((long) expected.size()));
                for (int i = 0; i < expected.size(); i++) {
                    int key = expected.get(i);
                    assertThat(hash.getKey1(i), equalTo(key));
                    assertThat(hash.getKey2(i), equalTo(key + 1));
                    assertThat(hash.getKey3(i), equalTo(key + 2));
                    assertThat(hash.find(key, key + 1, key + 2), equalTo((long) i));
                }
            } catch (CircuitBreakingException e) {
                // constructing the hash can trip too, which leaves nothing to verify on this pass
                assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
            }
        }
        assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testSimple() {
        try (Int3Hash hash = randomHash()) {
            assertThat(hash.add(0, 0, 0), equalTo(0L));
            assertThat(hash.add(0, 0, 1), equalTo(1L));
            assertThat(hash.add(0, 1, 1), equalTo(2L));
            assertThat(hash.add(1, 0, 0), equalTo(3L));
            assertThat(hash.add(1, 0, 1), equalTo(4L));

            assertThat(hash.add(0, 0, 0), equalTo(-1L));
            assertThat(hash.add(0, 0, 1), equalTo(-2L));
            assertThat(hash.add(1, 0, 1), equalTo(-5L));

            assertThat(hash.getKey1(0), equalTo(0));
            assertThat(hash.getKey2(0), equalTo(0));
            assertThat(hash.getKey3(0), equalTo(0));
            assertThat(hash.getKey1(4), equalTo(1));
            assertThat(hash.getKey2(4), equalTo(0));
            assertThat(hash.getKey3(4), equalTo(1));
        }
    }

    public void testDuel() {
        try (Int3Hash hash = randomHash()) {
            int iters = scaledRandomIntBetween(100, 100000);
            Key[] values = randomArray(1, iters, Key[]::new, () -> new Key(randomInt(), randomInt(), randomInt()));
            Map<Key, Integer> keyToId = new HashMap<>();
            List<Key> idToKey = new ArrayList<>();
            for (int i = 0; i < iters; ++i) {
                Key key = randomFrom(values);
                if (keyToId.containsKey(key)) {
                    assertEquals(-1 - keyToId.get(key), hash.add(key.key1, key.key2, key.key3));
                } else {
                    assertEquals(keyToId.size(), hash.add(key.key1, key.key2, key.key3));
                    keyToId.put(key, keyToId.size());
                    idToKey.add(key);
                }
            }

            assertEquals(keyToId.size(), hash.size());
            for (Map.Entry<Key, Integer> entry : keyToId.entrySet()) {
                assertEquals(entry.getValue().longValue(), hash.find(entry.getKey().key1, entry.getKey().key2, entry.getKey().key3));
            }

            assertEquals(idToKey.size(), hash.size());
            for (long i = 0; i < hash.capacity(); i++) {
                long id = hash.id(i);
                if (id >= 0) {
                    Key key = idToKey.get((int) id);
                    assertEquals(key.key1, hash.getKey1(id));
                    assertEquals(key.key2, hash.getKey2(id));
                    assertEquals(key.key3, hash.getKey3(id));
                }
            }

            for (long i = 0; i < hash.size(); i++) {
                Key key = idToKey.get((int) i);
                assertEquals(key.key1, hash.getKey1(i));
                assertEquals(key.key2, hash.getKey2(i));
                assertEquals(key.key3, hash.getKey3(i));
            }
        }
    }

    public void testAllocation() {
        MockBigArrays.assertFitsIn(ByteSizeValue.ofBytes(256), bigArrays -> new Int3Hash(1, bigArrays));
    }

    record Key(int key1, int key2, int key3) {

    }
}
