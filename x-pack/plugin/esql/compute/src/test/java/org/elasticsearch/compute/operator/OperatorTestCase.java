/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.AssumptionViolatedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

/**
 * Base tests for all operators.
 */
public abstract class OperatorTestCase extends ESTestCase {

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
     * The operator configured a "simple" or basic way, used for smoke testing
     * descriptions and {@link BigArrays} and scatter/gather.
     */
    protected abstract Operator.OperatorFactory simple(BigArrays bigArrays);

    /**
     * Valid input to be sent to {@link #simple};
     */
    protected abstract SourceOperator simpleInput(int size);

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
     * Assert that output from {@link #simple} is correct for the
     * given input.
     */
    protected abstract void assertSimpleOutput(List<Page> input, List<Page> results);

    /**
     * A {@link ByteSizeValue} that is so small any input to the operator
     * will cause it to circuit break. If the operator can't break then
     * throw an {@link AssumptionViolatedException}.
     */
    protected abstract ByteSizeValue smallEnoughToCircuitBreak();

    /**
     * Test a small input set against {@link #simple}. Smaller input sets
     * are more likely to discover accidental behavior for clumped inputs.
     */
    public final void testSimpleSmallInput() {
        assertSimple(nonBreakingBigArrays(), between(10, 100));
    }

    /**
     * Test a larger input set against {@link #simple}.
     */
    public final void testSimpleLargeInput() {
        assertSimple(nonBreakingBigArrays(), between(1_000, 10_000));
    }

    /**
     * Run {@link #simple} with a circuit breaker configured by
     * {@link #smallEnoughToCircuitBreak} and assert that it breaks
     * in a sane way.
     */
    public final void testSimpleCircuitBreaking() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, smallEnoughToCircuitBreak());
        Exception e = expectThrows(CircuitBreakingException.class, () -> assertSimple(bigArrays, between(1_000, 10_000)));
        assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
    }

    /**
     * Run {@link #simple} with the {@link CrankyCircuitBreakerService}
     * which fails randomly. This will catch errors caused by not
     * properly cleaning up things like {@link BigArray}s, particularly
     * in ctors.
     */
    public final void testSimpleWithCranky() {
        CrankyCircuitBreakerService breaker = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker).withCircuitBreaking();
        try {
            assertSimple(bigArrays, between(1_000, 10_000));
            // Either we get lucky and cranky doesn't throw and the test completes or we don't and it throws
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    /**
     * Makes sure the description of {@link #simple} matches the {@link #expectedDescriptionOfSimple}.
     */
    public final void testSimpleDescription() {
        Operator.OperatorFactory factory = simple(nonBreakingBigArrays());
        String description = factory.describe();
        assertThat(description, equalTo(expectedDescriptionOfSimple()));
        DriverContext driverContext = new DriverContext();
        try (Operator op = factory.get(driverContext)) {
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
        try (Operator operator = simple(nonBreakingBigArrays()).get(new DriverContext())) {
            assertThat(operator.toString(), equalTo(expectedToStringOfSimple()));
        }
    }

    /**
     * A {@link BigArrays} that won't throw {@link CircuitBreakingException}.
     */
    protected final BigArrays nonBreakingBigArrays() {
        return new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()).withCircuitBreaking();
    }

    /**
     * Run the {@code operators} once per page in the {@code input}.
     */
    protected final List<Page> oneDriverPerPage(List<Page> input, Supplier<List<Operator>> operators) {
        return oneDriverPerPageList(input.stream().map(List::of).iterator(), operators);
    }

    /**
     * Run the {@code operators} once to entry in the {@code source}.
     */
    protected final List<Page> oneDriverPerPageList(Iterator<List<Page>> source, Supplier<List<Operator>> operators) {
        List<Page> result = new ArrayList<>();
        while (source.hasNext()) {
            List<Page> in = source.next();
            try (
                Driver d = new Driver(
                    new DriverContext(),
                    new CannedSourceOperator(in.iterator()),
                    operators.get(),
                    new PageConsumerOperator(result::add),
                    () -> {}
                )
            ) {
                d.run();
            }
        }
        return result;
    }

    private void assertSimple(BigArrays bigArrays, int size) {
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(size));
        List<Page> results = drive(simple(bigArrays.withCircuitBreaking()).get(new DriverContext()), input.iterator());
        assertSimpleOutput(input, results);
    }

    protected final List<Page> drive(Operator operator, Iterator<Page> input) {
        return drive(List.of(operator), input);
    }

    protected final List<Page> drive(List<Operator> operators, Iterator<Page> input) {
        List<Page> results = new ArrayList<>();
        try (
            Driver d = new Driver(
                new DriverContext(),
                new CannedSourceOperator(input),
                operators,
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        return results;
    }

    public static void assertDriverContext(DriverContext driverContext) {
        assertTrue(driverContext.isFinished());
        assertThat(driverContext.getSnapshot().releasables(), empty());
    }
}
