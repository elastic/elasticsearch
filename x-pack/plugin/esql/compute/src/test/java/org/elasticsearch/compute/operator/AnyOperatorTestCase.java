/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

/**
 * Superclass for testing any {@link Operator}, including {@link SourceOperator}s.
 */
public abstract class AnyOperatorTestCase extends ComputeTestCase {
    /**
     * The operator configured a "simple" or basic way, used for smoke testing
     * descriptions, {@link CircuitBreaker}s, and scatter/gather.
     */
    protected abstract Operator.OperatorFactory simple();

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
        Operator.OperatorFactory factory = simple();
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
        try (Operator operator = simple().get(driverContext())) {
            assertThat(operator.toString(), equalTo(expectedToStringOfSimple()));
        }
    }

    /**
     * A {@link DriverContext} with a nonBreakingBigArrays.
     */
    protected DriverContext driverContext() { // TODO make this final once all operators support memory tracking
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory);
    }

    protected final DriverContext crankyDriverContext() {
        BlockFactory blockFactory = crankyBlockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory);
    }
}
