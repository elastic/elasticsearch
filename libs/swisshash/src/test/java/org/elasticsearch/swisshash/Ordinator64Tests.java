/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 20)
public class Ordinator64Tests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        for (AddType addType : AddType.values()) {
            params.add(new Object[] { addType, "tiny", 5, 0, 1, 1 });
            params.add(new Object[] { addType, "small", Ordinator64.INITIAL_CAPACITY / 2, 0, 1, 1 });
            params.add(new Object[] { addType, "two key pages", PageCacheRecycler.PAGE_SIZE_IN_BYTES / Long.BYTES, 1, 2, 1 });
            params.add(new Object[] { addType, "two id pages", PageCacheRecycler.PAGE_SIZE_IN_BYTES / Integer.BYTES, 2, 4, 2 });
            params.add(new Object[] { addType, "many", PageCacheRecycler.PAGE_SIZE_IN_BYTES, 4, 16, 8 });
            params.add(new Object[] { addType, "huge", 100_000, 6, 64, 32 });
        }
        return params;
    }

    private enum AddType {
        SINGLE_VALUE,
        ARRAY,
        // todo potentially remove this entirely, same as in ordinator64
        // BUILDER;
    }

    private final AddType addType;
    private final String name;
    private final int count;
    private final int expectedGrowCount;
    private final int expectedKeyPageCount;
    private final int expectedIdPageCount;

    public Ordinator64Tests(
        @Name("addType") AddType addType,
        @Name("name") String name,
        @Name("count") int count,
        @Name("expectedGrowCount") int expectedGrowCount,
        @Name("expectedKeyPageCount") int expectedKeyPageCount,
        @Name("expectedIdPageCount") int expectedIdPageCount
    ) {
        this.addType = addType;
        this.name = name;
        this.count = count;
        this.expectedGrowCount = expectedGrowCount;
        this.expectedKeyPageCount = expectedKeyPageCount;
        this.expectedIdPageCount = expectedIdPageCount;
    }

    public void testValues() {
        Set<Long> values = randomValues(count);
        long[] v = values.stream().mapToLong(Long::longValue).toArray();

        TestRecycler recycler = new TestRecycler();
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        try (Ordinator64 ord = new Ordinator64(recycler, breaker)) {
            assertThat(ord.size(), equalTo(0));

            switch (addType) {
                case SINGLE_VALUE -> {
                    for (int i = 0; i < v.length; i++) {
                        assertThat(ord.add(v[i]), equalTo(i));
                        assertThat(ord.size(), equalTo(i + 1));
                        assertThat(ord.get(i), equalTo(v[i]));
                        assertThat(ord.add(v[i]), equalTo(i));
                        assertThat(ord.size(), equalTo(i + 1));
                    }
                    for (int i = 0; i < v.length; i++) {
                        assertThat(ord.add(v[i]), equalTo(i));
                    }
                    assertThat(ord.size(), equalTo(v.length));
                }
                case ARRAY -> {
                    int[] target = new int[v.length];
                    ord.add(v, target, v.length);
                    assertThat(target, equalTo(IntStream.range(0, count).toArray()));
                    assertThat(ord.size(), equalTo(v.length));

                    Arrays.fill(target, 0);
                    ord.add(v, target, v.length);
                    assertThat(target, equalTo(IntStream.range(0, count).toArray()));
                    assertThat(ord.size(), equalTo(v.length));
                }
                // todo potentially remove this entirely, same as in ordinator64
                // case BUILDER -> {
                // LongBlock.Builder target = LongBlock.newBlockBuilder(count);
                // ord.add(v, target, v.length);
                // assertThat(target.build(), equalTo(new LongArrayVector(LongStream.range(0, count).toArray(), count).asBlock()));
                // assertThat(ord.currentSize(), equalTo(v.length));
                //
                // target = LongBlock.newBlockBuilder(count);
                // ord.add(v, target, v.length);
                // assertThat(target.build(), equalTo(new LongArrayVector(LongStream.range(0, count).toArray(), count).asBlock()));
                // assertThat(ord.currentSize(), equalTo(v.length));
                // }
                default -> throw new IllegalArgumentException();
            }
            for (int i = 0; i < v.length; i++) {
                assertThat(ord.find(v[i]), equalTo(i));
            }
            assertThat(ord.size(), equalTo(v.length));
            assertThat(ord.find(randomValueOtherThanMany(values::contains, ESTestCase::randomLong)), equalTo(-1));

            assertStatus(ord);
            assertThat("Only currently used pages are open", recycler.open, hasSize(expectedKeyPageCount + expectedIdPageCount));

            Long[] iterated = new Long[count];
            for (Ordinator64.Itr itr = ord.iterator(); itr.next();) {
                assertThat(iterated[itr.id()], nullValue());
                iterated[itr.id()] = itr.key();
            }
            for (int i = 0; i < v.length; i++) {
                assertThat(iterated[i], equalTo(v[i]));
            }
            // values densely pack into the keys array will be store in insertion order
            for (int i = 0; i < v.length; i++) {
                assertThat(ord.get(i), equalTo(v[i]));
            }
        }
        assertThat(recycler.open, hasSize(0));
    }

    public void testBreaker() {
        Set<Long> values = randomValues(count);
        long[] v = values.stream().mapToLong(Long::longValue).toArray();

        TestRecycler recycler = new TestRecycler();
        long breakAt = (expectedIdPageCount + expectedKeyPageCount) * PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        if (expectedGrowCount == 0) {
            breakAt -= 10;
        }
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("test", ByteSizeValue.ofBytes(breakAt));
        Exception e = expectThrows(CircuitBreakingException.class, () -> {
            try (Ordinator64 ord = new Ordinator64(recycler, breaker)) {
                switch (addType) {
                    case SINGLE_VALUE -> {
                        for (int i = 0; i < v.length; i++) {
                            assertThat(ord.add(v[i]), equalTo(i));
                        }
                    }
                    case ARRAY -> {
                        int[] target = new int[v.length];
                        ord.add(v, target, v.length);
                    }
                    // todo potentially remove this entirely, same as in ordinator64
                    // case BUILDER -> {
                    // LongBlock.Builder target = LongBlock.newBlockBuilder(count);
                    // ord.add(v, target, v.length);
                    // }
                    default -> throw new IllegalArgumentException();
                }
            }
        });
        assertThat(e.getMessage(), equalTo("over test limit"));
        assertThat(recycler.open, hasSize(0));
    }

    // High-probability bucket collisions. You just need structural patterns that
    // tend to collide in the bucket selection logic.

    public void testSameBucketCollisionsSmall() {
        testSameBucketCollisionsImpl(1000);
    }

    public void testSameBucketCollisionsBig() {
        testSameBucketCollisionsImpl(10000);
    }

    private void testSameBucketCollisionsImpl(int count) {
        TestRecycler recycler = new TestRecycler();
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        try (Ordinator64 ord = new Ordinator64(recycler, breaker)) {
            // mask must match the table mask used by Ordinator64
            int mask = 0xFFFF; // oversized; we only need lower bits locked
            long base = randomLong();

            long[] keys = makeSameBucketKeys(base, mask, count);

            Map<Long, Integer> expected = new HashMap<>();
            for (long k : keys) {
                int id = ord.add(k);
                expected.put(k, id);
            }

            // Verify lookups
            for (long k : keys) {
                assertThat(ord.find(k), equalTo(expected.get(k)));
            }
        }
    }

    public void testSameControlDataCollisionsSmall() {
        testSameControlDataCollisionsImpl(800);
        testSameControlDataCollisionsImpl(1000);
        testSameControlDataCollisionsImpl(1200);
    }

    public void testSameControlDataCollisionsBig() {
        testSameControlDataCollisionsImpl(3000);
        testSameControlDataCollisionsImpl(10000);
        testSameControlDataCollisionsImpl(20000);
    }

    private void testSameControlDataCollisionsImpl(int count) {
        TestRecycler recycler = new TestRecycler();
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        try (Ordinator64 ord = new Ordinator64(recycler, breaker)) {
            int control = randomIntBetween(1, 120); // avoid EMPTY/SENTINEL values
            long[] keys = makeSameControlDataKeys(control, count);
            Map<Long, Integer> expected = new HashMap<>();
            for (long k : keys) {
                int id = ord.add(k);
                expected.put(k, id);
            }

            // All must be findable despite metadata aliasing
            for (long k : keys) {
                assertThat(ord.find(k), equalTo(expected.get(k)));
            }

            // Check iteration completeness
            int seen = 0;
            var itr = ord.iterator();
            while (itr.next()) {
                assertTrue(expected.containsKey(itr.key()));
                seen++;
            }
            assertThat(seen, equalTo(expected.size()));
        }
    }

    public void testWorstCaseCollisionClusterSmall() {
        testWorstCaseCollisionClusterImpl(1000);  // small core
    }

    public void testWorstCaseCollisionClusterBig() {
        testWorstCaseCollisionClusterImpl(3000);  // big core
        testWorstCaseCollisionClusterImpl(5000);
        testWorstCaseCollisionClusterImpl(10000);
    }

    private void testWorstCaseCollisionClusterImpl(int count) {
        TestRecycler recycler = new TestRecycler();
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        try (Ordinator64 ord = new Ordinator64(recycler, breaker)) {
            // Pick a fixed 7-bit metadata and fixed low bits.
            int control = randomIntBetween(1, 120);
            long fixedBucketBits = randomLong() & 0xFFFF; // lock bucket range

            long[] keys = new long[count];
            for (int i = 0; i < count; i++) {
                long upper = ((long) control) << (64 - 7);
                long mid = ((long) i) << 16;       // differing mid bits
                long lower = fixedBucketBits;
                keys[i] = upper | mid | lower;
            }

            Map<Long, Integer> expected = new HashMap<>();
            for (long k : keys) {
                int id = ord.add(k);
                expected.put(k, id);
            }

            // Validate correctness
            for (long k : keys) {
                assertThat(ord.find(k), equalTo(expected.get(k)));
            }

            // Validate iteration covers all keys
            int total = 0;
            var itr = ord.iterator();
            while (itr.next()) {
                long key = itr.key();
                assertTrue("Iteration returned unexpected key " + key, expected.containsKey(key));
                total++;
            }
            assertThat(total, equalTo(count));
        }
    }

    private long[] makeSameBucketKeys(long base, int mask, int count) {
        long[] result = new long[count];
        for (int i = 0; i < count; i++) {
            // Force same bucket: (hash(x) & mask) = fixed value.
            // Here we simply mutate upper bits while leaving lower bits constant.
            result[i] = (base & mask) | ((long) i << 32);
        }
        return result;
    }

    private long[] makeSameControlDataKeys(int controlValue, int count) {
        long[] result = new long[count];
        for (int i = 0; i < count; i++) {
            long upper = (long) controlValue << (64 - 7);
            long lower = randomLong() & ((1L << (64 - 7)) - 1);
            result[i] = upper | lower;
        }
        return result;
    }

    private void assertStatus(Ordinator64 ord) {
        Ordinator.Status status = ord.status();
        assertThat(status.size(), equalTo(count));
        if (expectedGrowCount == 0) {
            assertThat(status.growCount(), equalTo(0));
            assertThat(status.capacity(), equalTo(Ordinator64.INITIAL_CAPACITY));
            assertThat(status.nextGrowSize(), equalTo((int) (Ordinator64.INITIAL_CAPACITY * Ordinator64.SmallCore.FILL_FACTOR)));
        } else {
            assertThat(status.growCount(), equalTo(expectedGrowCount));
            assertThat(status.capacity(), equalTo(Ordinator64.INITIAL_CAPACITY << expectedGrowCount));
            assertThat(
                status.nextGrowSize(),
                equalTo((int) ((Ordinator64.INITIAL_CAPACITY << expectedGrowCount) * Ordinator64.BigCore.FILL_FACTOR))
            );

            Ordinator.BigCoreStatus s = (Ordinator.BigCoreStatus) status;
            assertThat(s.keyPages(), equalTo(expectedKeyPageCount));
            assertThat(s.idPages(), equalTo(expectedIdPageCount));
        }
    }

    private Set<Long> randomValues(int count) {
        Set<Long> values = new HashSet<>();
        while (values.size() < count) {
            values.add(randomLong());
        }
        return values;
    }

    static class TestRecycler extends PageCacheRecycler {
        private final List<MyV<?>> open = new ArrayList<>();

        TestRecycler() {
            super(Settings.EMPTY);
        }

        @Override
        public Recycler.V<byte[]> bytePage(boolean clear) {
            return new MyV<>(super.bytePage(clear));
        }

        @Override
        public Recycler.V<Object[]> objectPage() {
            return new MyV<>(super.objectPage());
        }

        class MyV<T> implements Recycler.V<T> {
            private final Recycler.V<T> delegate;

            MyV(Recycler.V<T> delegate) {
                this.delegate = delegate;
                open.add(this);
            }

            @Override
            public T v() {
                return delegate.v();
            }

            @Override
            public boolean isRecycled() {
                return delegate.isRecycled();
            }

            @Override
            public void close() {
                open.remove(this);
                delegate.close();
            }
        }
    }
}
