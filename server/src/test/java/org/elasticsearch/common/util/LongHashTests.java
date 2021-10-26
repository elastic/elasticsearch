/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class LongHashTests extends ESTestCase {
    private BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private LongHash randomHash() {
        // Test high load factors to make sure that collision resolution works fine
        float maxLoadFactor = 0.6f + randomFloat() * 0.39f;
        return new LongHash(randomIntBetween(0, 100), maxLoadFactor, mockBigArrays());
    }

    public void testDuell() {
        try (LongHash hash = randomHash()) {
            final Long[] values = new Long[randomIntBetween(1, 100000)];
            for (int i = 0; i < values.length; ++i) {
                values[i] = randomLong();
            }
            final LongLongMap valueToId = new LongLongHashMap();
            final long[] idToValue = new long[values.length];
            final int iters = randomInt(1000000);
            for (int i = 0; i < iters; ++i) {
                final Long value = randomFrom(values);
                if (valueToId.containsKey(value)) {
                    assertEquals(-1 - valueToId.get(value), hash.add(value));
                } else {
                    assertEquals(valueToId.size(), hash.add(value));
                    idToValue[valueToId.size()] = value;
                    valueToId.put(value, valueToId.size());
                }
            }

            assertEquals(valueToId.size(), hash.size());
            for (Iterator<LongLongCursor> iterator = valueToId.iterator(); iterator.hasNext(); ) {
                final LongLongCursor next = iterator.next();
                assertEquals(next.value, hash.find(next.key));
            }

            for (long i = 0; i < hash.capacity(); ++i) {
                final long id = hash.id(i);
                if (id >= 0) {
                    assertEquals(idToValue[(int) id], hash.get(id));
                }
            }

            for (long i = 0; i < hash.size(); i++) {
                assertEquals(idToValue[(int) i], hash.get(i));
            }
        }
    }

    public void testSize() {
        LongHash hash = randomHash();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            final int mod = 1 + randomInt(40);
            for (int i = 0; i < 797; i++) {
                long count = hash.size();
                long key = hash.add(randomLong());
                if (key < 0)
                    assertEquals(hash.size(), count);
                else
                    assertEquals(hash.size(), count + 1);
                if (i % mod == 0) {
                    hash.close();
                    hash = randomHash();
                }
            }
        }
        hash.close();
    }

    public void testKey() {
        LongHash hash = randomHash();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Map<Long, Long> longs = new HashMap<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                long ref = randomLong();
                long count = hash.size();
                long key = hash.add(ref);
                if (key >= 0) {
                    assertNull(longs.put(ref, key));
                    assertEquals(uniqueCount, key);
                    uniqueCount++;
                    assertEquals(hash.size(), count + 1);
                } else {
                    assertTrue((-key) - 1L < count);
                    assertEquals(hash.size(), count);
                }
            }

            for (Map.Entry<Long, Long> entry : longs.entrySet()) {
                long expected = entry.getKey();
                long keyIdx = entry.getValue();
                assertEquals(expected, hash.get(keyIdx));
            }

            hash.close();
            hash = randomHash();
        }
        hash.close();
    }

    public void testAdd() {
        LongHash hash = randomHash();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Set<Long> longs = new HashSet<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                long ref = randomLong();
                long count = hash.size();
                long key = hash.add(ref);
                if (key >= 0) {
                    assertTrue(longs.add(ref));
                    assertEquals(uniqueCount, key);
                    assertEquals(hash.size(), count + 1);
                    uniqueCount++;
                } else {
                    assertFalse(longs.add(ref));
                    assertTrue((-key) - 1 < count);
                    assertEquals(ref, hash.get((-key) - 1));
                    assertEquals(count, hash.size());
                }
            }

            assertAllIn(longs, hash);
            hash.close();
            hash = randomHash();
        }
        hash.close();
    }

    public void testFind() throws Exception {
        LongHash hash = randomHash();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Set<Long> longs = new HashSet<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                long ref = randomLong();
                long count = hash.size();
                long key = hash.find(ref);
                if (key >= 0) { // found in hash
                    assertFalse(longs.add(ref));
                    assertTrue(key < count);
                    assertEquals(ref, hash.get(key));
                    assertEquals(count, hash.size());
                } else {
                    key = hash.add(ref);
                    assertTrue(longs.add(ref));
                    assertEquals(uniqueCount, key);
                    assertEquals(hash.size(), count + 1);
                    uniqueCount++;
                }
            }

            assertAllIn(longs, hash);
            hash.close();
            hash = randomHash();
        }
        hash.close();
    }

    public void testAllocation() {
        MockBigArrays.assertFitsIn(new ByteSizeValue(160), bigArrays -> new LongHash(1, bigArrays));
    }

    private static void assertAllIn(Set<Long> longs, LongHash hash) {
        long count = hash.size();
        for (Long l : longs) {
            long key = hash.add(l); // add again to check duplicates
            assertEquals(l.longValue(), hash.get((-key) - 1));
            assertEquals(count, hash.size());
            assertTrue("key: " + key + " count: " + count + " long: " + l, key < count);
        }
    }
}
