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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class OrdinatorBytesTests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        // name, count, expectedGrowCount, expectedIdPageCount
        params.add(new Object[] { "tiny", 5, 0, 1 });
        params.add(new Object[] { "small", OrdinatorBytes.INITIAL_CAPACITY / 2, 0, 1 });
        params.add(new Object[] { "two id pages", PageCacheRecycler.PAGE_SIZE_IN_BYTES / Integer.BYTES, 1, 2 });
        params.add(new Object[] { "many", PageCacheRecycler.PAGE_SIZE_IN_BYTES, 3, 8 });
        params.add(new Object[] { "huge", 100_000, 5, 32 });
        return params;
    }

    private final String name;
    private final int count;
    private final int expectedGrowCount;
    private final int expectedIdPageCount;

    public OrdinatorBytesTests(
        @Name("name") String name,
        @Name("count") int count,
        @Name("expectedGrowCount") int expectedGrowCount,
        @Name("expectedIdPageCount") int expectedIdPageCount
    ) {
        this.name = name;
        this.count = count;
        this.expectedGrowCount = expectedGrowCount;
        this.expectedIdPageCount = expectedIdPageCount;
    }

    public void testValues() {
        Set<BytesRef> values = randomValues(count);
        BytesRef[] v = values.toArray(new BytesRef[0]);

        TestRecycler recycler = new TestRecycler();
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        BigArrays bigArrays = new MockBigArrays(recycler, ByteSizeValue.ofBytes(Long.MAX_VALUE));

        try (OrdinatorBytes ord = new OrdinatorBytes(recycler, breaker, bigArrays)) {
            assertThat(ord.size(), equalTo(0));

            for (int i = 0; i < v.length; i++) {
                assertThat(ord.add(v[i]), equalTo(i));
                assertThat(ord.size(), equalTo(i + 1));
                assertThat(ord.add(v[i]), equalTo(i));
                assertThat(ord.size(), equalTo(i + 1));
            }
            for (int i = 0; i < v.length; i++) {
                assertThat(ord.add(v[i]), equalTo(i));
            }
            assertThat(ord.size(), equalTo(v.length));

            for (int i = 0; i < v.length; i++) {
                assertThat(ord.find(v[i]), equalTo(i));
            }
            assertThat(ord.size(), equalTo(v.length));

            BytesRef other = new BytesRef("not_in_set");
            while (values.contains(other)) {
                other = new BytesRef(other.utf8ToString() + "_");
            }
            assertThat(ord.find(other), equalTo(-1));

            assertStatus(ord);
            // Note: we cannot easily assert recycler.open size because BigArrays (BytesRefArray) usage
            // is mixed with Ordinator's usage. Ordinator uses explicit pages for IDs.

            BytesRef[] iterated = new BytesRef[count];
            BytesRef scratch = new BytesRef();
            for (OrdinatorBytes.Itr itr = ord.iterator(); itr.next();) {
                assertThat(iterated[itr.id()], nullValue());
                iterated[itr.id()] = BytesRef.deepCopyOf(itr.key(scratch));
            }
            for (int i = 0; i < v.length; i++) {
                assertThat(iterated[i], equalTo(v[i]));
            }
            // values densely pack into the keys array will be store in insertion order
            for (int i = 0; i < v.length; i++) {
                assertThat(ord.get(i, scratch), equalTo(v[i]));
            }
        }
        assertThat(recycler.open, hasSize(0));
    }

    public void testSharedBytesRefArray() {
        Set<BytesRef> leftValues = randomValues(count);
        Set<BytesRef> rightValues = randomValues(count);
        BytesRef[] left = leftValues.toArray(new BytesRef[0]);
        BytesRef[] right = rightValues.toArray(new BytesRef[0]);

        TestRecycler recycler = new TestRecycler();
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        BigArrays bigArrays = new MockBigArrays(recycler, ByteSizeValue.ofBytes(Long.MAX_VALUE));

        try (BytesRefArray sharedArray = new BytesRefArray(PageCacheRecycler.PAGE_SIZE_IN_BYTES, bigArrays)) {
            try (
                OrdinatorBytes leftOrd = new OrdinatorBytes(recycler, breaker, sharedArray);
                OrdinatorBytes rightOrd = new OrdinatorBytes(recycler, breaker, sharedArray);
            ) {
                assertThat(leftOrd.size(), equalTo(0));
                assertThat(rightOrd.size(), equalTo(0));

                for (int i = 0; i < count; i++) {
                    // Add to left
                    int idLeft = leftOrd.add(left[i]);
                    // Add to right
                    int idRight = rightOrd.add(right[i]);

                    assertThat(idLeft, equalTo(2 * i));
                    assertThat(idRight, equalTo(2 * i + 1));
                }

                assertThat(leftOrd.size(), equalTo(count));
                assertThat(rightOrd.size(), equalTo(count));

                for (int i = 0; i < count; i++) {
                    assertThat(leftOrd.find(left[i]), equalTo(2 * i));
                    assertThat(rightOrd.find(right[i]), equalTo(2 * i + 1));
                }

                assertStatus(leftOrd);
                assertStatus(rightOrd);
            }
        }
        assertThat(recycler.open, hasSize(0));
    }

    public void testBreaker() {
        Set<BytesRef> values = randomValues(count);
        BytesRef[] v = values.toArray(new BytesRef[0]);

        TestRecycler recycler = new TestRecycler();
        // Break based on ID pages.
        // We don't easily know BytesRefArray usage here, but we can constrain the breaker enough to fail.
        long breakAt = (expectedIdPageCount) * PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        if (expectedGrowCount == 0) {
            // Tiny/Small cases might fit or be tight.
            // Reduce slightly to force break if we allocate anything extra.
            breakAt = Math.max(1, breakAt - 100);
        }

        // Note: BigArrays also uses the breaker.
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("test", ByteSizeValue.ofBytes(breakAt));
        BigArrays bigArrays = new MockBigArrays(recycler, ByteSizeValue.ofBytes(Long.MAX_VALUE));

        Exception e = expectThrows(CircuitBreakingException.class, () -> {
            try (OrdinatorBytes ord = new OrdinatorBytes(recycler, breaker, bigArrays)) {
                for (int i = 0; i < v.length; i++) {
                    ord.add(v[i]);
                }
            }
        });
        assertThat(e.getMessage(), equalTo("over test limit"));
        assertThat(recycler.open, hasSize(0));
    }

    private void assertStatus(OrdinatorBytes ord) {
        Ordinator.Status status = ord.status();

        if (expectedGrowCount == 0) {
            // In small core, capacity is fixed.
            assertThat(status.growCount(), equalTo(0));
            assertThat(status.capacity(), equalTo(OrdinatorBytes.INITIAL_CAPACITY));
        } else {
            assertThat(status.growCount(), equalTo(expectedGrowCount));
            assertThat(status.capacity(), equalTo(OrdinatorBytes.INITIAL_CAPACITY << expectedGrowCount));

            Ordinator.BigCoreStatus s = (Ordinator.BigCoreStatus) status;
            assertThat(s.idPages(), equalTo(expectedIdPageCount));
            // We don't assert keyPages because OrdinatorBytes doesn't track them (BytesRefArray does)
        }
    }

    private Set<BytesRef> randomValues(int count) {
        Set<BytesRef> values = new HashSet<>();
        while (values.size() < count) {
            byte[] bytes = new byte[randomIntBetween(1, 20)];
            random().nextBytes(bytes);
            values.add(new BytesRef(bytes));
        }
        return values;
    }

    static class TestRecycler extends PageCacheRecycler {
        final List<MyV<?>> open = new ArrayList<>();

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
