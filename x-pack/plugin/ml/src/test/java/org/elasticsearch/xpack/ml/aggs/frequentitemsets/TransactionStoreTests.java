/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
            store = new TransactionStore(mockBigArraysWithThrowingCircuitBreaker());
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
        TransactionStore store = new TransactionStore(mockBigArrays());
        addRandomTransactions(store, randomIntBetween(100, 10000));

        boolean thrown = false;
        TransactionStore storeCopy = null;
        try {
            // cbe's are thrown at random, test teardown checks if all buffers have been closed
            storeCopy = copyInstance(
                store,
                writableRegistry(),
                (out, value) -> value.writeTo(out),
                in -> new TransactionStore(in, mockBigArraysWithThrowingCircuitBreaker()),
                Version.CURRENT
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

    private void addRandomTransactions(TransactionStore store, int n) {
        List<String> randomFields = Arrays.asList(
            generateRandomStringArray(randomIntBetween(5, 50), randomIntBetween(5, 50), false, false)
        );

        for (int i = 0; i < n; ++i) {
            store.add(randomFields.stream().filter(p -> randomBoolean()).map(f -> {
                List<Object> l = new ArrayList<Object>(
                    Arrays.asList(generateRandomStringArray(randomIntBetween(1, 10), randomIntBetween(5, 50), false, false))
                );
                return new Tuple<String, List<Object>>(f, l);
            }));
        }
    }

}
