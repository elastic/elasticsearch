/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class IntNHashTests extends ESTestCase {
    private BigArrays randombigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    public void test1Key() {
        LongHash hash1 = new LongHash(between(1, 100), randombigArrays());
        IntNHash hash2 = new IntNHash(randomIntBetween(0, 100), 1, randombigArrays());
        IntNHash hash3 = new IntNHash(randomIntBetween(0, 100), 2, randombigArrays());
        int padding = randomInt();
        int values = between(10, 1000);
        for (int n = 0; n < values; n++) {
            int value = randomIntBetween(0, 1000);
            long id1 = hash1.add(value);
            long id2 = hash2.add(new int[] { value });
            long id3 = hash3.add(new int[] { value, padding });
            assertThat(id1, equalTo(id2));
            assertThat(id1, equalTo(id3));
        }
        assertThat(hash1.size(), equalTo(hash2.size));
        assertThat(hash2.size(), equalTo(hash3.size));
        for (long l = 0; l < hash1.size(); l++) {
            int v1 = (int) hash1.get(l);
            int v2 = hash2.getKeys(l)[0];
            int v3 = hash3.getKeys(l)[0];
            assertThat(v1, equalTo(v2));
            assertThat(v1, equalTo(v3));
        }
        Releasables.close(hash1, hash2, hash3);
    }

    public void test2Keys() {
        LongHash hash1 = new LongHash(between(1, 100), randombigArrays());
        IntNHash hash2 = new IntNHash(randomIntBetween(0, 100), 2, randombigArrays());
        IntNHash hash3 = new IntNHash(randomIntBetween(0, 100), 3, randombigArrays());
        int padding = randomInt();
        int values = between(10, 1000);
        for (int n = 0; n < values; n++) {
            int first = randomIntBetween(0, 1000);
            int second = randomIntBetween(0, 1000);
            long v = (((long) first) << 32) | (second & 0xFFFFFFFFL);
            long id1 = hash1.add(v);
            long id2 = hash2.add(new int[] { first, second });
            long id3 = hash3.add(new int[] { first, second, padding });
            assertThat(id1, equalTo(id2));
            assertThat(id1, equalTo(id3));
        }
        assertThat(hash1.size(), equalTo(hash2.size));
        assertThat(hash2.size(), equalTo(hash3.size()));
        for (long l = 0; l < hash1.size(); l++) {
            long v1 = hash1.get(l);
            int first = (int) (v1 >>> 32);
            int second = (int) (v1 & 0xFFFFFFFFL);
            int[] v2 = hash2.getKeys(l);
            assertThat(v2, equalTo(new int[] { first, second }));
            int[] v3 = hash3.getKeys(l);
            assertThat(v3, equalTo(new int[] { first, second, padding }));
        }
        Releasables.close(hash1, hash2, hash3);
    }

    public void test3Keys() {
        Int3Hash hash1 = new Int3Hash(between(1, 100), randombigArrays());
        IntNHash hash2 = new IntNHash(randomIntBetween(0, 100), 3, randombigArrays());
        IntNHash hash3 = new IntNHash(randomIntBetween(0, 100), 4, randombigArrays());
        int values = between(10, 1000);
        int padding = randomInt();
        for (int n = 0; n < values; n++) {
            int v1 = randomIntBetween(0, 1000);
            int v2 = randomIntBetween(0, 1000);
            int v3 = randomIntBetween(0, 1000);
            long id1 = hash1.add(v1, v2, v3);
            long id2 = hash2.add(new int[] { v1, v2, v3 });
            long id3 = hash3.add(new int[] { v1, v2, v3, padding });
            assertThat(id1, equalTo(id2));
            assertThat(id1, equalTo(id3));
        }
        assertThat(hash1.size(), equalTo(hash2.size));
        assertThat(hash2.size(), equalTo(hash3.size()));
        for (long l = 0; l < hash1.size; l++) {
            int v1 = hash1.getKey1(l);
            int v2 = hash1.getKey2(l);
            int v3 = hash1.getKey3(l);
            assertThat(hash2.getKeys(l), equalTo(new int[] { v1, v2, v3 }));
            assertThat(hash3.getKeys(l), equalTo(new int[] { v1, v2, v3, padding }));
        }
        Releasables.close(hash1, hash2, hash3);
    }

    public void test4Keys() {
        LongLongHash hash1 = new LongLongHash(between(1, 100), randombigArrays());
        IntNHash hash2 = new IntNHash(randomIntBetween(0, 100), 4, randombigArrays());
        IntNHash hash3 = new IntNHash(randomIntBetween(0, 100), 5, randombigArrays());
        int padding = randomInt();
        int values = between(10, 1000);
        for (int n = 0; n < values; n++) {
            int v1 = randomIntBetween(0, 1000);
            int v2 = randomIntBetween(0, 1000);
            int v3 = randomIntBetween(0, 1000);
            int v4 = randomIntBetween(0, 1000);
            long id1 = hash1.add((((long) v1) << 32) | (v2 & 0xFFFFFFFFL), (((long) v3) << 32) | (v4 & 0xFFFFFFFFL));
            long id2 = hash2.add(new int[] { v1, v2, v3, v4 });
            long id3 = hash3.add(new int[] { v1, v2, v3, v4, padding });
            assertThat(id1, equalTo(id2));
            assertThat(id1, equalTo(id3));
        }
        assertThat(hash1.size(), equalTo(hash2.size));
        assertThat(hash1.size(), equalTo(hash3.size));
        for (long l = 0; l < hash1.size; l++) {
            long k1 = hash1.getKey1(l);
            long k2 = hash1.getKey2(l);
            int v1 = (int) (k1 >>> 32);
            int v2 = (int) (k1 & 0xFFFFFFFFL);
            int v3 = (int) (k2 >>> 32);
            int v4 = (int) (k2 & 0xFFFFFFFFL);
            assertThat(hash2.getKeys(l), equalTo(new int[] { v1, v2, v3, v4 }));
            assertThat(hash3.getKeys(l), equalTo(new int[] { v1, v2, v3, v4, padding }));
        }
        Releasables.close(hash1, hash2, hash3);
    }

    public void testLargeKeys() {
        record Ints(int[] vs) {
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Ints key = (Ints) o;
                return Arrays.equals(vs, key.vs);
            }

            @Override
            public int hashCode() {
                return Arrays.hashCode(vs);
            }
        }
        Map<Ints, Long> maps = new HashMap<>();
        int keySize = randomIntBetween(1, 50);
        int values = between(1, 1000);
        try (IntNHash hash = new IntNHash(randomIntBetween(0, 1000), keySize, randombigArrays())) {
            for (int i = 0; i < values; i++) {
                int[] keys = new int[keySize];
                for (int k = 0; k < keySize; k++) {
                    keys[k] = randomIntBetween(0, 1000);
                }
                long hashId = hash.add(keys);
                Ints ints = new Ints(keys);
                if (hashId < 0) {
                    hashId = -1 - hashId;
                    assertThat(maps.get(ints), equalTo(hashId));
                } else {
                    assertNull(maps.get(ints));
                    maps.put(ints, hashId);
                }
                assertThat((int) hash.size, equalTo(maps.size()));
            }
            for (long l = 0; l < hash.size; l++) {
                int[] key = hash.getKeys(l);
                Ints ints = new Ints(key);
                assertThat(maps.get(ints), equalTo(l));
            }
            for (var e : maps.entrySet()) {
                long id = hash.find(e.getKey().vs);
                assertThat(id, equalTo(e.getValue()));
            }
        }
    }
}
