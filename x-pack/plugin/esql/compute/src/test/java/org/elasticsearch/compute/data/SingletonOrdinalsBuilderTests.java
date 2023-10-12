/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.equalTo;

public class SingletonOrdinalsBuilderTests extends ESTestCase {
    public void testAppend() {
        // NOCOMMIT real tests
        try (SingletonOrdinalsBuilder builder = new SingletonOrdinalsBuilder(breakingDriverContext().blockFactory(), null, 5)) {
            builder.appendInt(0);
            builder.appendInt(0);
            builder.appendInt(1);
            builder.appendInt(2);
        }
    }

    public void testCompactWithNulls() {
        assertCompactToUnique(new int[] { -1, -1, -1, -1, 0, 1, 2 }, List.of(0, 1, 2));
    }

    public void testCompactNoNulls() {
        assertCompactToUnique(new int[] { 0, 1, 2 }, List.of(0, 1, 2));
    }

    public void testCompactDups() {
        assertCompactToUnique(new int[] { 0, 0, 0, 1, 2 }, List.of(0, 1, 2));
    }

    public void testCompactSkips() {
        assertCompactToUnique(new int[] { 2, 7, 1000 }, List.of(2, 7, 1000));
    }

    private void assertCompactToUnique(int[] sortedOrds, List<Integer> expected) {
        int uniqueLength = SingletonOrdinalsBuilder.compactToUnique(sortedOrds);
        assertMap(Arrays.stream(sortedOrds).mapToObj(Integer::valueOf).limit(uniqueLength).toList(), matchesList(expected));
    }

    private final List<CircuitBreaker> breakers = new ArrayList<>();
    private final List<BlockFactory> blockFactories = new ArrayList<>();

    /**
     * A {@link DriverContext} with a breaking {@link BigArrays} and {@link BlockFactory}.
     */
    protected DriverContext breakingDriverContext() { // TODO move this to driverContext once everyone supports breaking
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        BlockFactory factory = new MockBlockFactory(breaker, bigArrays);
        blockFactories.add(factory);
        return new DriverContext(bigArrays, factory);
    }

    @After
    public void allBreakersEmpty() throws Exception {
        // first check that all big arrays are released, which can affect breakers
        MockBigArrays.ensureAllArraysAreReleased();

        for (CircuitBreaker breaker : breakers) {
            for (var factory : blockFactories) {
                if (factory instanceof MockBlockFactory mockBlockFactory) {
                    mockBlockFactory.ensureAllBlocksAreReleased();
                }
            }
            assertThat("Unexpected used in breaker: " + breaker, breaker.getUsed(), equalTo(0L));
        }
    }

}
