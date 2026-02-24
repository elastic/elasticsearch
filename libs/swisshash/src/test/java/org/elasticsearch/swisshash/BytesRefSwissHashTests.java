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

public class BytesRefSwissHashTests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        for (AddType addType : AddType.values()) {
            // addType, name, count, expectedGrowCount, expectedIdPageCount
            params.add(new Object[] { addType, "tiny", 5, 0, 1 });
            params.add(new Object[] { addType, "small", BytesRefSwissHash.INITIAL_CAPACITY / 2, 0, 1 });
            params.add(new Object[] { addType, "two idAndHash pages", PageCacheRecycler.PAGE_SIZE_IN_BYTES / Long.BYTES, 1, 2 });
            params.add(new Object[] { addType, "many", PageCacheRecycler.PAGE_SIZE_IN_BYTES, 4, 16 });
            params.add(new Object[] { addType, "huge", 100_000, 6, 64 });
        }
        return params;
    }

    private enum AddType {
        SINGLE_VALUE,
        // ARRAY // at some point, we might add bulk add operations
    }

    private final AddType addType;
    private final String name;
    private final int count;
    private final int expectedGrowCount;
    private final int expectedIdPageCount;

    public BytesRefSwissHashTests(
        @Name("addType") AddType addType,
        @Name("name") String name,
        @Name("count") int count,
        @Name("expectedGrowCount") int expectedGrowCount,
        @Name("expectedIdPageCount") int expectedIdPageCount
    ) {
        this.addType = addType;
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
        BytesRef scratch = new BytesRef();

        try (BytesRefSwissHash hash = new BytesRefSwissHash(recycler, breaker, bigArrays)) {
            assertThat(hash.size(), equalTo(0L));

            switch (addType) {
                case SINGLE_VALUE -> {
                    for (int i = 0; i < v.length; i++) {
                        assertThat(hash.add(v[i]), equalTo((long) i));
                        assertThat(hash.size(), equalTo(i + 1L));
                        assertThat(hash.get(i, scratch), equalTo(v[i]));
                        assertThat(hash.add(v[i]), equalTo(-1L - i));
                        assertThat(hash.size(), equalTo(i + 1L));
                    }
                    for (int i = 0; i < v.length; i++) {
                        assertThat(hash.add(v[i]), equalTo(-1L - i));
                    }
                    assertThat(hash.size(), equalTo((long) v.length));
                }
                default -> throw new IllegalArgumentException();
            }

            for (int i = 0; i < v.length; i++) {
                assertThat(hash.find(v[i]), equalTo((long) i));
            }
            assertThat(hash.size(), equalTo((long) v.length));

            BytesRef other = new BytesRef("not_in_set");
            while (values.contains(other)) {
                other = new BytesRef(other.utf8ToString() + "_");
            }
            assertThat(hash.find(other), equalTo(-1L));

            assertStatus(hash);
            // Note: we cannot easily assert recycler.open size because BigArrays (BytesRefArray) usage
            // is mixed with SwissHash's usage. SwissHash uses explicit pages for IDs.

            BytesRef[] iterated = new BytesRef[count];
            for (BytesRefSwissHash.Itr itr = hash.iterator(); itr.next();) {
                assertThat(iterated[itr.id()], nullValue());
                iterated[itr.id()] = BytesRef.deepCopyOf(itr.key(scratch));
            }
            for (int i = 0; i < v.length; i++) {
                assertThat(iterated[i], equalTo(v[i]));
            }
            // values densely pack into the keys array will be store in insertion order
            for (int i = 0; i < v.length; i++) {
                assertThat(hash.get(i, scratch), equalTo(v[i]));
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

        try (
            BytesRefArray sharedArray = new BytesRefArray(PageCacheRecycler.PAGE_SIZE_IN_BYTES, bigArrays);
            BytesRefSwissHash leftHash = new BytesRefSwissHash(recycler, breaker, sharedArray);
            BytesRefSwissHash rightHash = new BytesRefSwissHash(recycler, breaker, sharedArray)
        ) {
            assertThat(leftHash.size(), equalTo(0L));
            assertThat(rightHash.size(), equalTo(0L));

            for (int i = 0; i < count; i++) {
                // Add to left
                long idLeft = leftHash.add(left[i]);
                // Add to right
                long idRight = rightHash.add(right[i]);

                assertThat(idLeft, equalTo(2L * i));
                assertThat(idRight, equalTo(2L * i + 1));
            }

            assertThat(leftHash.size(), equalTo((long) count));
            assertThat(rightHash.size(), equalTo((long) count));

            for (int i = 0; i < count; i++) {
                assertThat(leftHash.find(left[i]), equalTo(2L * i));
                assertThat(rightHash.find(right[i]), equalTo(2L * i + 1));
            }

            assertStatus(leftHash);
            assertStatus(rightHash);
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
            try (BytesRefSwissHash hash = new BytesRefSwissHash(recycler, breaker, bigArrays)) {
                for (int i = 0; i < v.length; i++) {
                    hash.add(v[i]);
                }
            }
        });
        assertThat(e.getMessage(), equalTo("over test limit"));
        assertThat(recycler.open, hasSize(0));
    }

    public void testEmpty() {
        TestRecycler recycler = new TestRecycler();
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        BigArrays bigArrays = new MockBigArrays(recycler, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        try (BytesRefSwissHash hash = new BytesRefSwissHash(recycler, breaker, bigArrays)) {
            assertThat(hash.size(), equalTo(0L));
            assertFalse(hash.iterator().next());
        }
    }

    private void assertStatus(BytesRefSwissHash hash) {
        SwissHash.Status status = hash.status();

        if (expectedGrowCount == 0) {
            // In small core, capacity is fixed.
            assertThat(status.growCount(), equalTo(0));
            assertThat(status.capacity(), equalTo(BytesRefSwissHash.INITIAL_CAPACITY));
        } else {
            assertThat(status.growCount(), equalTo(expectedGrowCount));
            assertThat(status.capacity(), equalTo(BytesRefSwissHash.INITIAL_CAPACITY << expectedGrowCount));

            SwissHash.BigCoreStatus s = (SwissHash.BigCoreStatus) status;
            assertThat(s.idPages(), equalTo(expectedIdPageCount));
            // We don't assert keyPages because BytesRefSwissHash doesn't track them (BytesRefArray does)
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
