/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

/**
 * Superclass for testing any {@link Operator}, including {@link SourceOperator}s.
 */
public abstract class AnyOperatorTestCase extends ESTestCase {
    /**
     * The operator configured a "simple" or basic way, used for smoke testing
     * descriptions and {@link BigArrays} and scatter/gather.
     */
    protected abstract Operator.OperatorFactory simple(BigArrays bigArrays);

    /**
     * The description of the operator produced by {@link #simple}.
     */
    protected abstract String expectedDescriptionOfSimple();

    /**
     * The {@link #toString} of the operator produced by {@link #simple}.
     * This {@linkplain #toString} is used by the status reporting and
     * generally useful debug information.
     */
    protected abstract String expectedToStringOfSimple();

    /**
     * the description of an Operator should be "OperatorName(additional info)"
     * eg. "LimitOperator(limit = 10)"
     * Additional info are optional
     */
    private static final String OPERATOR_DESCRIBE_PATTERN = "^\\w*\\[.*\\]$";

    /**
     * the name a grouping agg function should be "aggName of type" for typed aggregations, eg. "avg of ints"
     * or "aggName" for type agnostic aggregations, eg. "count"
     */
    private static final String GROUPING_AGG_FUNCTION_DESCRIBE_PATTERN = "^\\w*( of \\w*$)?";

    /**
     * Makes sure the description of {@link #simple} matches the {@link #expectedDescriptionOfSimple}.
     */
    public final void testSimpleDescription() {
        Operator.OperatorFactory factory = simple(nonBreakingBigArrays());
        String description = factory.describe();
        assertThat(description, equalTo(expectedDescriptionOfSimple()));
        try (Operator op = factory.get(driverContext())) {
            if (op instanceof GroupingAggregatorFunction) {
                assertThat(description, matchesPattern(GROUPING_AGG_FUNCTION_DESCRIBE_PATTERN));
            } else {
                assertThat(description, matchesPattern(OPERATOR_DESCRIBE_PATTERN));
            }
        }
    }

    /**
     * Makes sure the description of {@link #simple} matches the {@link #expectedDescriptionOfSimple}.
     */
    public final void testSimpleToString() {
        try (Operator operator = simple(nonBreakingBigArrays()).get(driverContext())) {
            assertThat(operator.toString(), equalTo(expectedToStringOfSimple()));
        }
    }

    /**
     * A {@link BigArrays} that won't throw {@link CircuitBreakingException}.
     * <p>
     *     Rather than using the {@link NoneCircuitBreakerService} we use a
     *     very large limit so tests can call {@link CircuitBreaker#getUsed()}.
     * </p>
     */
    protected final BigArrays nonBreakingBigArrays() {
        return new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(Integer.MAX_VALUE)).withCircuitBreaking();
    }

    /**
     * A {@link DriverContext} with a nonBreakingBigArrays.
     */
    protected DriverContext driverContext() { // TODO make this final and return a breaking block factory
        return new DriverContext(nonBreakingBigArrays(), BlockFactory.getNonBreakingInstance());
    }
}
