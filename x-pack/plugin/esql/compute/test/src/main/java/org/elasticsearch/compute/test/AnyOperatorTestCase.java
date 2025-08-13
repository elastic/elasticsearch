/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;

/**
 * Superclass for testing any {@link Operator}, including {@link SourceOperator}s.
 */
public abstract class AnyOperatorTestCase extends ComputeTestCase {
    /**
     * @param requiresDeterministicFactory
     *          True if the returned {@link Operator.OperatorFactory} should always generate an identical deterministic operator.
     *          That is, for two different calls, both operators should do "exactly" the same.
     */
    protected record SimpleOptions(boolean requiresDeterministicFactory) {
        public static final SimpleOptions DEFAULT = new SimpleOptions(false);
    }

    /**
     * The operator configured a "simple" or basic way, used for smoke testing
     * descriptions, {@link CircuitBreaker}s, and scatter/gather.
     */
    protected abstract Operator.OperatorFactory simple(SimpleOptions options);

    /**
     * Calls {@link #simple(SimpleOptions)} with the default options.
     */
    protected final Operator.OperatorFactory simple() {
        return simple(SimpleOptions.DEFAULT);
    }

    /**
     * The description of the operator produced by {@link #simple}.
     */
    protected abstract Matcher<String> expectedDescriptionOfSimple();

    /**
     * The {@link #toString} of the operator produced by {@link #simple}.
     * This {@linkplain #toString} is used by the status reporting and
     * generally useful debug information.
     */
    protected abstract Matcher<String> expectedToStringOfSimple();

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
        assertThat(description, expectedDescriptionOfSimple());
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
            assertThat(operator.toString(), expectedToStringOfSimple());
        }
    }

    /**
     * Ensures that the Operator.Status of this operator has the standard fields, set to 0.
     */
    public void testEmptyOperatorStatus() {
        DriverContext driverContext = driverContext();
        try (var operator = simple().get(driverContext)) {
            assertOperatorStatus(operator, List.of(), List.of());
        }
    }

    /**
     * Assert the operator has a status, and its values are acceptable.
     * Delegates specific checks to {link #checkOperatorStatusFields}, which may be overridden.
     */
    protected void assertOperatorStatus(Operator operator, List<Page> input, List<Page> output) {
        Operator.Status status = operator.status();

        if (status == null) {
            // Operator doesn't provide a status
            return;
        }

        var xContent = XContentType.JSON.xContent();
        try (var xContentBuilder = XContentBuilder.builder(xContent)) {
            status.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            var bytesReference = BytesReference.bytes(xContentBuilder);
            var map = XContentHelper.convertToMap(bytesReference, false, xContentBuilder.contentType()).v2();

            // For some operators, we don't know here if they consumed/processed all the input.
            // That check should be done in the operator test
            var nonNegativeMatcher = input.isEmpty() ? matchNumberEqualTo(0) : matchNumberGreaterThanOrEqualTo(0);
            var inputPagesMatcher = matchNumberEqualTo(input.size());
            var totalInputRows = input.stream().mapToLong(Page::getPositionCount).sum();
            var inputRowsMatcher = matchNumberEqualTo(totalInputRows);
            var outputPagesMatcher = matchNumberEqualTo(output.size());
            var totalOutputRows = output.stream().mapToLong(Page::getPositionCount).sum();
            var outputRowsMatcher = matchNumberEqualTo(totalOutputRows);

            if (operator instanceof SourceOperator) {
                assertThat(map, hasEntry(is("pages_emitted"), outputPagesMatcher));
                assertThat(map, hasEntry(is("rows_emitted"), outputRowsMatcher));
            } else if (operator instanceof SinkOperator) {
                assertThat(map, hasEntry(is("pages_received"), inputPagesMatcher));
                assertThat(map, hasEntry(is("rows_received"), inputRowsMatcher));
            } else if (operator instanceof AsyncOperator) {
                assertThat(map, hasEntry(is("received_pages"), nonNegativeMatcher));
                assertThat(map, hasEntry(is("completed_pages"), nonNegativeMatcher));
                assertThat(map, hasEntry(is("process_nanos"), nonNegativeMatcher));
            } else {
                assertThat(
                    map,
                    Matchers.<Map<String, Object>>either(hasEntry(is("pages_processed"), nonNegativeMatcher))
                        .or(
                            Matchers.<Map<String, Object>>both(hasEntry(is("pages_received"), nonNegativeMatcher))
                                .and(hasEntry(is("pages_emitted"), outputPagesMatcher))
                        )
                );
                assertThat(map, hasEntry(is("rows_received"), nonNegativeMatcher));
                assertThat(map, hasEntry(is("rows_emitted"), outputRowsMatcher));
            }

            checkOperatorStatusFields(map, input, output);
        } catch (IOException e) {
            fail(e, "Failed to convert operator status to XContent");
        }
    }

    protected final Matcher<Object> matchNumberEqualTo(Number value) {
        return wholeMatcher(comparesEqualTo(value.intValue()), comparesEqualTo(value.longValue()));
    }

    protected final Matcher<Object> matchNumberGreaterThanOrEqualTo(Number value) {
        return wholeMatcher(greaterThanOrEqualTo(value.intValue()), greaterThanOrEqualTo(value.longValue()));
    }

    @SuppressWarnings("unchecked")
    protected final Matcher<Object> wholeMatcher(Matcher<Integer> integerMatcher, Matcher<Long> longMatcher) {
        return either(both(instanceOf(Integer.class)).and((Matcher<? super Object>) (Matcher<?>) integerMatcher)).or(
            both(instanceOf(Long.class)).and((Matcher<? super Object>) (Matcher<?>) longMatcher)
        );
    }

    /**
     * Tests the non-standard operator status fields.
     * <p>
     *     The standard fields (already tested in the generic test) are:
     * </p>
    *     <ul>
    *         <li>pages_received</li>
    *         <li>rows_received</li>
    *         <li>pages_processed</li>
    *         <li>pages_emitted</li>
    *         <li>rows_emitted</li>
    *     </ul>
     * <p>
     *     To be overridden by subclasses.
     * </p>
     * @param status The XContent map representation of the status.
     */
    protected void checkOperatorStatusFields(Map<String, Object> status, List<Page> input, List<Page> output) {}

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
