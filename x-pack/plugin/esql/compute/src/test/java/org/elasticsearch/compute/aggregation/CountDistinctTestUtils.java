/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;

import java.util.function.Consumer;

final class CountDistinctTestUtils {
    static final int PRECISION = 40000;

    private CountDistinctTestUtils() {}

    /**
     * The count the {@code COUNT_DISTINCT} aggregators must produce for the values fed to {@code collect}.
     * HLL++ is approximate even below the precision threshold: linear counting keeps only a 25-bit hash
     * prefix, so distinct values can collide. Counting with the same hashing and estimate as the aggregator
     * makes those collisions part of the expectation, so tests can assert an exact result.
     */
    static long expectedCount(Consumer<HllStates.SingleState> collect) {
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
        try (HllStates.SingleState state = new HllStates.SingleState(driverContext, PRECISION)) {
            collect.accept(state);
            return state.cardinality();
        }
    }
}
