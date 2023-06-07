/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class BytesRefHashTests extends ESTestCase {

    private static class ThrowingCircuitBreakerService extends CircuitBreakerService {

        private static class ThrowingCircuitBreaker implements CircuitBreaker {
            private final int startThrowingAfter;
            private int count = 0;

            ThrowingCircuitBreaker(int startThrowingAfter) {
                this.startThrowingAfter = startThrowingAfter;
            }

            @Override
            public void circuitBreak(String fieldName, long bytesNeeded) {}

            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                if (count >= startThrowingAfter) {
                    throw new CircuitBreakingException("cbe after " + count + " call(s)", Durability.PERMANENT);
                }
                ++count;
            }

            @Override
            public void addWithoutBreaking(long bytes) {}

            @Override
            public long getUsed() {
                return 0;
            }

            @Override
            public long getLimit() {
                return 0;
            }

            @Override
            public double getOverhead() {
                return 0;
            }

            @Override
            public long getTrippedCount() {
                return 0;
            }

            @Override
            public String getName() {
                return CircuitBreaker.FIELDDATA;
            }

            @Override
            public Durability getDurability() {
                return null;
            }

            @Override
            public void setLimitAndOverhead(long limit, double overhead) {

            }
        };

        private final CircuitBreaker breaker;

        ThrowingCircuitBreakerService(int startThrowingAfter) {
            breaker = new ThrowingCircuitBreaker(startThrowingAfter);
        }

        @Override
        public CircuitBreaker getBreaker(String name) {
            return breaker;
        }

        @Override
        public AllCircuitBreakerStats stats() {
            return new AllCircuitBreakerStats(new CircuitBreakerStats[] { stats(CircuitBreaker.FIELDDATA) });
        }

        @Override
        public CircuitBreakerStats stats(String name) {
            return new CircuitBreakerStats(CircuitBreaker.FIELDDATA, -1, -1, 0, 0);
        }
    }

    private BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private BigArrays mockBigArraysWithThrowingCircuitBreaker() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new ThrowingCircuitBreakerService(0)).withCircuitBreaking();
    }

    private BigArrays mockBigArraysWithThrowingCircuitBreakerAfterOneAllocation() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new ThrowingCircuitBreakerService(1)).withCircuitBreaking();
    }

    private BytesRefHash randomHash() {
        // Test high load factors to make sure that collision resolution works fine
        final float maxLoadFactor = 0.6f + randomFloat() * 0.39f;
        return new BytesRefHash(randomIntBetween(0, 100), maxLoadFactor, mockBigArrays());
    }

    public void testDuel() {
        try (BytesRefHash hash = randomHash()) {
            final int len = randomIntBetween(1, 100000);
            final BytesRef[] values = new BytesRef[len];
            for (int i = 0; i < values.length; ++i) {
                values[i] = new BytesRef(randomAlphaOfLength(5));
            }
            final Map<BytesRef, Integer> valueToId = new HashMap<>();
            final BytesRef[] idToValue = new BytesRef[values.length];
            final int iters = randomInt(1000000);
            for (int i = 0; i < iters; ++i) {
                final BytesRef value = randomFrom(values);
                if (valueToId.containsKey(value)) {
                    assertEquals(-1 - valueToId.get(value), hash.add(value, value.hashCode()));
                } else {
                    assertEquals(valueToId.size(), hash.add(value, value.hashCode()));
                    idToValue[valueToId.size()] = value;
                    valueToId.put(value, valueToId.size());
                }
            }

            assertEquals(valueToId.size(), hash.size());
            for (var entry : valueToId.entrySet()) {
                assertEquals(entry.getValue().longValue(), hash.find(entry.getKey(), entry.getKey().hashCode()));
            }

            for (long i = 0; i < hash.capacity(); ++i) {
                final long id = hash.id(i);
                BytesRef spare = new BytesRef();
                if (id >= 0) {
                    hash.get(id, spare);
                    assertEquals(idToValue[(int) id], spare);
                }
            }
        }
    }

    // START - tests borrowed from LUCENE

    /**
     * Test method for {@link org.apache.lucene.util.BytesRefHash#size()}.
     */
    public void testSize() {
        BytesRefHash hash = randomHash();
        BytesRefBuilder ref = new BytesRefBuilder();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            final int mod = 1 + randomInt(40);
            for (int i = 0; i < 797; i++) {
                String str;
                do {
                    str = TestUtil.randomRealisticUnicodeString(random(), 1000);
                } while (str.length() == 0);
                ref.copyChars(str);
                long count = hash.size();
                long key = hash.add(ref.get());
                if (key < 0) {
                    assertEquals(hash.size(), count);
                } else {
                    assertEquals(hash.size(), count + 1);
                }
                if (i % mod == 0) {
                    hash.close();
                    hash = randomHash();
                }
            }
        }
        hash.close();
    }

    /**
     * Test method for
     * {@link org.apache.lucene.util.BytesRefHash#get(int, BytesRef)}
     * .
     */
    public void testGet() {
        BytesRefHash hash = randomHash();
        BytesRefBuilder ref = new BytesRefBuilder();
        BytesRef scratch = new BytesRef();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Map<String, Long> strings = new HashMap<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                String str;
                do {
                    str = TestUtil.randomRealisticUnicodeString(random(), 1000);
                } while (str.length() == 0);
                ref.copyChars(str);
                long count = hash.size();
                long key = hash.add(ref.get());
                if (key >= 0) {
                    assertNull(strings.put(str, key));
                    assertEquals(uniqueCount, key);
                    uniqueCount++;
                    assertEquals(hash.size(), count + 1);
                } else {
                    assertTrue((-key) - 1 < count);
                    assertEquals(hash.size(), count);
                }
            }
            for (Entry<String, Long> entry : strings.entrySet()) {
                ref.copyChars(entry.getKey());
                assertEquals(ref.get(), hash.get(entry.getValue(), scratch));
            }
            hash.close();
            hash = randomHash();
        }
        hash.close();
    }

    /**
     * Test method for
     * {@link org.apache.lucene.util.BytesRefHash#add(org.apache.lucene.util.BytesRef)}
     * .
     */
    public void testAdd() {
        BytesRefHash hash = randomHash();
        BytesRefBuilder ref = new BytesRefBuilder();
        BytesRef scratch = new BytesRef();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Set<String> strings = new HashSet<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                String str;
                do {
                    str = TestUtil.randomRealisticUnicodeString(random(), 1000);
                } while (str.length() == 0);
                ref.copyChars(str);
                long count = hash.size();
                long key = hash.add(ref.get());

                if (key >= 0) {
                    assertTrue(strings.add(str));
                    assertEquals(uniqueCount, key);
                    assertEquals(hash.size(), count + 1);
                    uniqueCount++;
                } else {
                    assertFalse(strings.add(str));
                    assertTrue((-key) - 1 < count);
                    assertEquals(str, hash.get((-key) - 1, scratch).utf8ToString());
                    assertEquals(count, hash.size());
                }
            }

            assertAllIn(strings, hash);
            hash.close();
            hash = randomHash();
        }
        hash.close();
    }

    public void testFind() {
        BytesRefHash hash = randomHash();
        BytesRefBuilder ref = new BytesRefBuilder();
        BytesRef scratch = new BytesRef();
        int num = scaledRandomIntBetween(2, 20);
        for (int j = 0; j < num; j++) {
            Set<String> strings = new HashSet<>();
            int uniqueCount = 0;
            for (int i = 0; i < 797; i++) {
                String str;
                do {
                    str = TestUtil.randomRealisticUnicodeString(random(), 1000);
                } while (str.length() == 0);
                ref.copyChars(str);
                long count = hash.size();
                long key = hash.find(ref.get()); // hash.add(ref);
                if (key >= 0) { // string found in hash
                    assertFalse(strings.add(str));
                    assertTrue(key < count);
                    assertEquals(str, hash.get(key, scratch).utf8ToString());
                    assertEquals(count, hash.size());
                } else {
                    key = hash.add(ref.get());
                    assertTrue(strings.add(str));
                    assertEquals(uniqueCount, key);
                    assertEquals(hash.size(), count + 1);
                    uniqueCount++;
                }
            }

            assertAllIn(strings, hash);
            hash.close();
            hash = randomHash();
        }
        hash.close();
    }

    private void assertAllIn(Set<String> strings, BytesRefHash hash) {
        BytesRefBuilder ref = new BytesRefBuilder();
        BytesRef scratch = new BytesRef();
        long count = strings.size();
        for (String string : strings) {
            ref.copyChars(string);
            long key = hash.add(ref.get()); // add again to check duplicates
            assertEquals(string, hash.get((-key) - 1, scratch).utf8ToString());
            assertEquals(count, hash.size());
            assertTrue("key: " + key + " count: " + count + " string: " + string, key < count);
        }
    }

    // END - tests borrowed from LUCENE

    public void testAllocation() {
        MockBigArrays.assertFitsIn(ByteSizeValue.ofBytes(512), bigArrays -> new BytesRefHash(1, bigArrays));
    }

    private void assertEquality(BytesRefHash original, BytesRefHash copy) {
        BytesRef scratch = new BytesRef();
        int numberOfOriginalKeys = 0;
        int numberOfCopyKeys = 0;

        assertEquals(original.size(), copy.size);

        // check that all keys of original can be found in the copy
        for (int i = 0; i < original.capacity(); ++i) {
            long id = original.id(i);

            if (id != -1) {
                ++numberOfOriginalKeys;
                original.get(id, scratch);
                assertTrue(copy.find(scratch) >= 0);
            }
        }

        // count number of keys in copy
        for (int i = 0; i < copy.capacity(); ++i) {
            long id = copy.id(i);

            if (id != -1) {
                ++numberOfCopyKeys;
            }
        }

        assertEquals(numberOfOriginalKeys, numberOfCopyKeys);
    }

    public void testGetByteRefsAndSerialization() throws IOException {
        BytesRefHash hash = randomHash();
        BytesRefBuilder ref = new BytesRefBuilder();
        int runs = randomIntBetween(2, 20);

        for (int j = 0; j < runs; j++) {
            Set<String> strings = new HashSet<>();
            int entries = randomIntBetween(100, 5000);

            for (int i = 0; i < entries; i++) {
                String str = randomUnicodeOfLengthBetween(4, 20);
                ref.copyChars(str);
                hash.add(ref.get());
                strings.add(str);
            }

            BytesRefArray refArray = hash.getBytesRefs();

            BytesRefArray refArrayCopy = copyInstance(
                refArray,
                writableRegistry(),
                (out, value) -> value.writeTo(out),
                in -> new BytesRefArray(in, mockBigArrays()),
                    TransportVersion.current()
            );

            BytesRefHash copy = new BytesRefHash(refArrayCopy, mockBigArrays());
            refArrayCopy.close();
            assertEquality(hash, copy);
            assertAllIn(strings, copy);
            copy.close();
            hash.close();
            hash = randomHash();
        }
        hash.close();
    }

    // demonstrates how a given BytesRefArray must be protected against leakage
    public void testDontLeakBytesRefArray() {
        BytesRefArray array = null;
        boolean success = false;
        boolean thrown = false;
        BytesRefHash hash = null;
        try {
            // create a big array, so it is not allocated using ordinary java arrays
            array = BytesRefArrayTests.randomArray(
                PageCacheRecycler.LONG_PAGE_SIZE + 100,
                PageCacheRecycler.LONG_PAGE_SIZE + 100,
                mockBigArrays()
            );
            hash = new BytesRefHash(array, mockBigArraysWithThrowingCircuitBreaker());
            success = true;
        } catch (CircuitBreakingException ce) {
            assertEquals("cbe after 0 call(s)", ce.getMessage());
            thrown = true;
        } finally {
            if (false == success) {
                // if you have more than 1 resource:
                // try (Releasable releasable = Releasables.wrap(r1, r2, ...)) { ... }
                Releasables.close(array);
            }
        }

        assertFalse(success);
        assertTrue(thrown);
        assertNotNull(array);
        assertNull(hash);
    }

    // demonstrates how a given BytesRefArray must be protected against leakage, special case
    public void testDontLeakBytesRefArray2() {
        BytesRefArray array = null;
        boolean success = false;
        boolean thrown = false;
        BytesRefHash hash = null;
        try {
            // create a big array, so it is not allocated using ordinary java arrays
            array = BytesRefArrayTests.randomArray(
                PageCacheRecycler.LONG_PAGE_SIZE + 100,
                PageCacheRecycler.LONG_PAGE_SIZE + 100,
                mockBigArrays()
            );
            hash = new BytesRefHash(array, mockBigArraysWithThrowingCircuitBreakerAfterOneAllocation());
            success = true;
        } catch (CircuitBreakingException ce) {
            assertEquals("cbe after 1 call(s)", ce.getMessage());
            thrown = true;
        } finally {
            if (false == success) {
                // if you have more than 1 resource:
                // try (Releasable releasable = Releasables.wrap(r1, r2, ...)) { ... }
                Releasables.close(array);
            }
        }

        assertFalse(success);
        assertTrue(thrown);
        assertNotNull(array);
        assertNull(hash);
    }

    // demonstrates how a given BytesRefArray must be protected against leakage, special case when the array creation already throws
    public void testDontLeakBytesRefArray3() {
        BytesRefArray array = null;
        boolean success = false;
        boolean thrown = false;
        BytesRefHash hash = null;
        try {
            // create a big array, so it is not allocated using ordinary java arrays
            array = BytesRefArrayTests.randomArray(
                PageCacheRecycler.LONG_PAGE_SIZE + 100,
                PageCacheRecycler.LONG_PAGE_SIZE + 100,
                mockBigArraysWithThrowingCircuitBreaker()
            );
            hash = new BytesRefHash(array, mockBigArrays());
            success = true;
        } catch (CircuitBreakingException ce) {
            assertEquals("cbe after 0 call(s)", ce.getMessage());
            thrown = true;
        } finally {
            if (false == success) {
                // if you have more than 1 resource:
                // try (Releasable releasable = Releasables.wrap(r1, r2, ...)) { ... }
                Releasables.close(array);
            }
        }

        assertFalse(success);
        assertTrue(thrown);
        assertNull(array);
        assertNull(hash);
    }
}
