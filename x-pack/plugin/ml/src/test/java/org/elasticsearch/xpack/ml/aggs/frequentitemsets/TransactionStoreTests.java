/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSourceTests.createKeywordFieldTestInstance;

public class TransactionStoreTests extends ESTestCase {

    private static class ThrowingCircuitBreakerService extends CircuitBreakerService {

        private static class RandomlyThrowingCircuitBreaker implements CircuitBreaker {

            @Override
            public void circuitBreak(String fieldName, long bytesNeeded) {}

            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                if (random().nextInt(20) == 0) {
                    throw new CircuitBreakingException("cbe", Durability.PERMANENT);
                }
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

        private final CircuitBreaker breaker = new RandomlyThrowingCircuitBreaker();

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
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new ThrowingCircuitBreakerService()).withCircuitBreaking();
    }

    public void testDontLeak() {
        TransactionStore store = null;
        boolean thrown = false;
        try {
            // cbe's are thrown at random, test teardown checks if all buffers have been closed
            store = new HashBasedTransactionStore(mockBigArraysWithThrowingCircuitBreaker());
        } catch (CircuitBreakingException ce) {
            assertEquals("cbe", ce.getMessage());
            thrown = true;
        }

        if (thrown == false) {
            assertNotNull(store);
            store.close();
        }
    }

    public void testDontLeakAfterDeserializing() throws IOException {
        HashBasedTransactionStore store = new HashBasedTransactionStore(mockBigArrays());
        addRandomTransactions(store, randomIntBetween(100, 10000));

        boolean thrown = false;
        TransactionStore storeCopy = null;
        try {
            // cbe's are thrown at random, test teardown checks if all buffers have been closed
            storeCopy = copyInstance(
                store,
                writableRegistry(),
                (out, value) -> value.writeTo(out),
                in -> new HashBasedTransactionStore(in, mockBigArraysWithThrowingCircuitBreaker()),
                TransportVersion.current()
            );
        } catch (CircuitBreakingException ce) {
            assertEquals("cbe", ce.getMessage());
            thrown = true;
        }

        if (thrown == false) {
            assertNotNull(store);
            storeCopy.close();
        }
        store.close();
    }

    public void testCreateImmutableTransactionStore() throws IOException {
        HashBasedTransactionStore store = new HashBasedTransactionStore(mockBigArrays());
        addRandomTransactions(store, randomIntBetween(100, 10000));

        // create a copy
        HashBasedTransactionStore storeCopy = copyInstance(
            store,
            writableRegistry(),
            (out, value) -> value.writeTo(out),
            in -> new HashBasedTransactionStore(in, mockBigArrays()),
            TransportVersion.current()
        );

        // create an immutable version
        ImmutableTransactionStore immutableStore = storeCopy.createImmutableTransactionStore();
        storeCopy.close();

        assertEquality(store, immutableStore);

        immutableStore.close();
        store.close();
    }

    public void testItemBow() {
        Tuple<Long, Long> item1 = tuple(0L, 2L);
        Tuple<Long, Long> item2 = tuple(1L, 2L);
        Tuple<Long, Long> item3 = tuple(2L, 1L);
        Tuple<Long, Long> item4 = tuple(3L, 3L);
        Tuple<Long, Long> item5 = tuple(4L, 4L);

        assertEquals(-1, HashBasedTransactionStore.ITEMS_BY_COUNT_COMPARATOR.compare(item1, item2));
        assertEquals(1, HashBasedTransactionStore.ITEMS_BY_COUNT_COMPARATOR.compare(item2, item1));
        assertEquals(-1, HashBasedTransactionStore.ITEMS_BY_COUNT_COMPARATOR.compare(item1, item3));
        assertEquals(1, HashBasedTransactionStore.ITEMS_BY_COUNT_COMPARATOR.compare(item3, item5));

        List<Tuple<Long, Long>> items = new ArrayList<>();

        items.add(item1);
        items.add(item2);
        items.add(item3);
        items.add(item4);
        items.add(item5);

        items.sort(HashBasedTransactionStore.ITEMS_BY_COUNT_COMPARATOR);

        assertEquals(List.of(item5, item4, item1, item2, item3), items);

        // test that compareItems produces exactly the same order
        try (LongArray itemCounts = mockBigArrays().newLongArray(5)) {
            itemCounts.set(0, item1.v2());
            itemCounts.set(1, item2.v2());
            itemCounts.set(2, item3.v2());
            itemCounts.set(3, item4.v2());
            itemCounts.set(4, item5.v2());

            List<Long> itemsAsList = new ArrayList<>();
            itemsAsList.add(item1.v1());
            itemsAsList.add(item2.v1());
            itemsAsList.add(item3.v1());
            itemsAsList.add(item4.v1());
            itemsAsList.add(item5.v1());

            itemsAsList.sort(HashBasedTransactionStore.compareItems(itemCounts));

            assertEquals(List.of(item5.v1(), item4.v1(), item1.v1(), item2.v1(), item3.v1()), itemsAsList);
        }
    }

    private void addRandomTransactions(HashBasedTransactionStore store, int n) {
        List<Field> randomFields = new ArrayList<>();
        String[] randomFieldNames = generateRandomStringArray(randomIntBetween(5, 50), randomIntBetween(5, 50), false, false);

        int id = 0;
        for (String fieldName : randomFieldNames) {
            randomFields.add(createKeywordFieldTestInstance(fieldName, id++));
        }

        for (int i = 0; i < n; ++i) {
            store.add(randomFields.stream().filter(p -> randomBoolean()).map(f -> {
                List<Object> l = new ArrayList<Object>(
                    Arrays.asList(generateRandomStringArray(randomIntBetween(1, 10), randomIntBetween(5, 50), false, false))
                );
                return new Tuple<Field, List<Object>>(f, l);
            }));
        }
    }

    private void assertEquality(TransactionStore original, TransactionStore copy) throws IOException {
        assertEquals(original.getTotalItemCount(), copy.getTotalItemCount());
        assertEquals(original.getTotalTransactionCount(), copy.getTotalTransactionCount());
        assertEquals(original.getUniqueItemsCount(), copy.getUniqueItemsCount());

        BytesRef orgBytesRef = new BytesRef();
        BytesRef copyBytesRef = new BytesRef();

        for (int i = 0; i < original.getUniqueItemsCount(); ++i) {
            original.getItem(i, orgBytesRef);
            copy.getItem(i, copyBytesRef);
            assertEquals(orgBytesRef, copyBytesRef);
        }

        for (int i = 0; i < original.getUniqueTransactionCount(); ++i) {
            original.getTransaction(i, orgBytesRef);
            copy.getTransaction(i, copyBytesRef);
            assertEquals(orgBytesRef, copyBytesRef);
        }
    }
}
