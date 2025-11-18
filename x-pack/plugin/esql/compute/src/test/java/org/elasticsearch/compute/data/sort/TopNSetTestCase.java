/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public abstract class TopNSetTestCase<T extends Releasable, V extends Comparable<V>> extends ESTestCase {
    /**
     * Build a {@link T} to test. Sorts built by this method shouldn't need scores.
     */
    protected abstract T build(BigArrays bigArrays, SortOrder sortOrder, int limit);

    private T build(SortOrder sortOrder, int limit) {
        return build(bigArrays(), sortOrder, limit);
    }

    /**
     * A random value for testing, with the appropriate precision for the type we're testing.
     */
    protected abstract V randomValue();

    /**
     * Returns a list of 3 values, in ascending order.
     */
    protected abstract List<V> threeSortedValues();

    /**
     * Collect a value into the top.
     *
     * @param value value to collect, always sent as double just to have
     *              a number to test. Subclasses should cast to their favorite types
     */
    protected abstract void collect(T sort, V value);

    protected abstract void reduceLimitByOne(T sort);

    protected abstract V getWorstValue(T sort);

    protected abstract int getCount(T sort);

    public final void testNeverCalled() {
        SortOrder sortOrder = randomFrom(SortOrder.values());
        int limit = randomIntBetween(0, 10);
        try (T sort = build(sortOrder, limit)) {
            assertResults(sort, sortOrder, limit, List.of());
        }
    }

    public final void testLimit0() {
        SortOrder sortOrder = randomFrom(SortOrder.values());
        int limit = 0;
        try (T sort = build(sortOrder, limit)) {
            var values = threeSortedValues();

            collect(sort, values.get(0));
            collect(sort, values.get(1));

            assertResults(sort, sortOrder, limit, List.of());
        }
    }

    public final void testSingleValue() {
        SortOrder sortOrder = randomFrom(SortOrder.values());
        int limit = 1;
        try (T sort = build(sortOrder, limit)) {
            var values = threeSortedValues();

            collect(sort, values.get(0));

            assertResults(sort, sortOrder, limit, List.of(values.get(0)));
        }
    }

    public final void testNonCompetitive() {
        SortOrder sortOrder = SortOrder.DESC;
        int limit = 1;
        try (T sort = build(sortOrder, limit)) {
            var values = threeSortedValues();

            collect(sort, values.get(1));
            collect(sort, values.get(0));

            assertResults(sort, sortOrder, limit, List.of(values.get(1)));
        }
    }

    public final void testCompetitive() {
        SortOrder sortOrder = SortOrder.DESC;
        int limit = 1;
        try (T sort = build(sortOrder, limit)) {
            var values = threeSortedValues();

            collect(sort, values.get(0));
            collect(sort, values.get(1));

            assertResults(sort, sortOrder, limit, List.of(values.get(1)));
        }
    }

    public final void testTwoHitsDesc() {
        SortOrder sortOrder = SortOrder.DESC;
        int limit = 2;
        try (T sort = build(sortOrder, limit)) {
            var values = threeSortedValues();

            collect(sort, values.get(0));
            collect(sort, values.get(1));
            collect(sort, values.get(2));

            assertResults(sort, sortOrder, limit, List.of(values.get(2), values.get(1)));
        }
    }

    public final void testTwoHitsAsc() {
        SortOrder sortOrder = SortOrder.ASC;
        int limit = 2;
        try (T sort = build(sortOrder, limit)) {
            var values = threeSortedValues();

            collect(sort, values.get(0));
            collect(sort, values.get(1));
            collect(sort, values.get(2));

            assertResults(sort, sortOrder, limit, List.of(values.get(0), values.get(1)));
        }
    }

    public final void testReduceLimit() {
        SortOrder sortOrder = randomFrom(SortOrder.values());
        int limit = 3;
        try (T sort = build(sortOrder, limit)) {
            var values = threeSortedValues();

            collect(sort, values.get(0));
            collect(sort, values.get(1));
            collect(sort, values.get(2));

            assertResults(sort, sortOrder, limit, values);

            reduceLimitByOne(sort);
            collect(sort, values.get(2));

            assertResults(sort, sortOrder, limit - 1, values);
        }
    }

    public final void testCrankyBreaker() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new CrankyCircuitBreakerService());
        SortOrder sortOrder = randomFrom(SortOrder.values());
        int limit = randomIntBetween(0, 3);

        try (T sort = build(bigArrays, sortOrder, limit)) {
            List<V> values = new ArrayList<>();

            for (int i = 0; i < randomIntBetween(0, 4); i++) {
                V value = randomValue();
                values.add(value);
                collect(sort, value);
            }

            if (randomBoolean() && limit > 0) {
                reduceLimitByOne(sort);
                limit--;

                V value = randomValue();
                values.add(value);
                collect(sort, value);
            }

            assertResults(sort, sortOrder, limit, values);
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
        assertThat(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    protected void assertResults(T sort, SortOrder sortOrder, int limit, List<V> values) {
        var sortedUniqueValues = values.stream()
            .distinct()
            .sorted(sortOrder == SortOrder.ASC ? Comparator.naturalOrder() : Comparator.reverseOrder())
            .limit(limit)
            .toList();

        assertEquals(sortedUniqueValues.size(), getCount(sort));
        if (sortedUniqueValues.isEmpty() == false) {
            assertEquals(sortedUniqueValues.getLast(), getWorstValue(sort));
        }
    }

    private BigArrays bigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }
}
