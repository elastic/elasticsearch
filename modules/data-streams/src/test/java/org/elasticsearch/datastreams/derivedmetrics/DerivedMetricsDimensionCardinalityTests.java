/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class DerivedMetricsDimensionCardinalityTests extends ESTestCase {

    private static final List<String> DIMENSIONS = List.of("service.name", "user.id");

    private CircuitBreakerService breakerService;
    private CircuitBreaker breaker;
    private BigArrays bigArrays;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        breakerService = LimitedBreaker.service(DerivedMetricsService.BREAKER_NAME, ByteSizeValue.ofMb(16));
        breaker = breakerService.getBreaker(DerivedMetricsService.BREAKER_NAME);
        // MockBigArrays fails the test if anything allocated here is not released, which is the leak these sketches could introduce
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), breakerService).withCircuitBreaking();
    }

    /**
     * The whole point of counting is to be able to say which dimension is spending the series budget, so the estimate has to be close
     * enough that an operator looking at two dimensions can tell which one is the problem. At precision 8 the relative error is around
     * 6.5%, and the bound here is deliberately wider than that so the test is about the sketch working at all rather than about the
     * particular values this seed happened to hash to.
     */
    public void testTheEstimateIsCloseToTheRealDistinctCount() {
        try (var cardinality = create(0)) {
            for (int i = 0; i < 5_000; i++) {
                cardinality.observe(new String[] { "service-" + (i % 7), "user-" + i });
            }
            long services = estimate(cardinality, 0);
            assertThat(
                "a seven-valued dimension is still in linear counting, which is near exact",
                services,
                both(greaterThan(5L)).and(lessThan(10L))
            );
            long users = estimate(cardinality, 1);
            logger.info("estimated [{}] distinct values for a dimension that really had 5000", users);
            assertThat(users, both(greaterThan(4_000L)).and(lessThan(6_000L)));
        }
        assertEquals("the sketches must give every byte back", 0L, breaker.getUsed());
    }

    /**
     * A dimension that stays well inside its budget must never collapse, however many observations it sees — otherwise the feature would
     * quietly destroy the breakdowns that are working.
     */
    public void testADimensionWithinItsBudgetIsNeverCollapsed() {
        try (var cardinality = create(1_000)) {
            for (int i = 0; i < 20_000; i++) {
                cardinality.observe(new String[] { "service-" + (i % 50), "user-" + (i % 100) });
            }
            assertEquals(0L, cardinality.collapsedMask());
            assertFalse(cardinality.collapsed(0));
            assertFalse(cardinality.collapsed(1));
        }
        assertEquals(0L, breaker.getUsed());
    }

    /**
     * The degradation this exists for: the runaway dimension is given up and the well-behaved one beside it is untouched, so the metric
     * keeps a breakdown worth having instead of losing the metric.
     */
    public void testOnlyTheRunawayDimensionCollapses() {
        int collapsed = 0;
        try (var cardinality = create(200)) {
            for (int i = 0; i < 2_000; i++) {
                collapsed += cardinality.observe(new String[] { "service-" + (i % 5), "user-" + i });
            }
            assertFalse("a five-valued dimension is nowhere near its budget", cardinality.collapsed(0));
            assertTrue("a dimension with two thousand values is far past it", cardinality.collapsed(1));
            assertEquals(0b10L, cardinality.collapsedMask());
            assertEquals("a dimension collapses once, not once per observation after it", 1, collapsed);
        }
        assertEquals(0L, breaker.getUsed());
    }

    /**
     * A budget of zero is how an operator asks for the diagnosis without the degradation: keep telling me which dimension is expensive,
     * but do not change what is emitted.
     */
    public void testAZeroBudgetCountsWithoutCollapsing() {
        try (var cardinality = create(0)) {
            for (int i = 0; i < 5_000; i++) {
                cardinality.observe(new String[] { "service-a", "user-" + i });
            }
            assertEquals(0L, cardinality.collapsedMask());
            assertThat(estimate(cardinality, 1), greaterThan(4_000L));
        }
        assertEquals(0L, breaker.getUsed());
    }

    /** A dimension the document did not have is not a value, so it must not inflate the count of the values that were there. */
    public void testAnAbsentDimensionIsNotAValue() {
        try (var cardinality = create(0)) {
            for (int i = 0; i < 100; i++) {
                cardinality.observe(new String[] { null, "user-" + i });
            }
            assertEquals(0L, estimate(cardinality, 0));
        }
        assertEquals(0L, breaker.getUsed());
    }

    /**
     * A metric with no dimensions has nothing to count, and must not pay a byte for the machinery that would have counted it — the
     * built-in ingest metrics are exactly this shape and they are on every write path that has the feature enabled at all.
     */
    public void testAMetricWithoutDimensionsCostsNothing() {
        DerivedMetricsDimensionCardinality cardinality = DerivedMetricsDimensionCardinality.create(bigArrays, breaker, List.of(), 100);
        assertSame(DerivedMetricsDimensionCardinality.DISABLED, cardinality);
        assertEquals(0, cardinality.observe(new String[0]));
        assertEquals(0L, cardinality.collapsedMask());
        assertEquals(0L, breaker.getUsed());
    }

    private DerivedMetricsDimensionCardinality create(int budget) {
        return DerivedMetricsDimensionCardinality.create(bigArrays, breaker, DIMENSIONS, budget);
    }

    private static long estimate(DerivedMetricsDimensionCardinality cardinality, int dimension) {
        return cardinality.estimatedValues(dimension);
    }
}
