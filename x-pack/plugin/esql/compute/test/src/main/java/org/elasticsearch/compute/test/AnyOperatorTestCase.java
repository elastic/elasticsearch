/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.notNullValue;

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
    public void testSimpleDescription() {
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
     * Tests that {@link Operator#canProduceMoreDataWithoutExtraInput()} returns false before the operator has started
     * (for non-source operators, before any input is added; for source operators, before any output is produced)
     * and after it is finished.
     */
    public void testCanProduceMoreDataWithoutExtraInput() {
        DriverContext driverContext = driverContext();
        try (var operator = simple().get(driverContext)) {
            // Before operator has started
            // For non-source operators: no input added yet - should return false
            // For source operators: they can produce data without input, so this may return true
            // We just verify the method doesn't throw
            boolean initialValue = operator.canProduceMoreDataWithoutExtraInput();
            if (operator instanceof SourceOperator == false) {
                assertFalse(
                    "canProduceMoreDataWithoutExtraInput should return false before operator has started (no input added)",
                    initialValue
                );
            }

            // After operator is finished - should return false for all operators
            operator.finish();
            // For async operators, wait for async actions to complete
            if (operator instanceof AsyncOperator<?>) {
                driverContext.finish();
                PlainActionFuture<Void> waitForAsync = new PlainActionFuture<>();
                driverContext.waitForAsyncActions(waitForAsync);
                try {
                    waitForAsync.actionGet(TimeValue.timeValueSeconds(30));
                } catch (Exception e) {
                    // Ignore exceptions - we just want to ensure async actions complete
                }
            }
            // Ensure operator is finished by draining any remaining output
            while (operator.isFinished() == false) {
                // Some operators need getOutput() to be called to finish
                Page output = operator.getOutput();
                if (output != null) {
                    output.releaseBlocks();
                } else {
                    break;
                }
            }
            assertTrue("Operator should be finished", operator.isFinished());
            assertFalse(
                "canProduceMoreDataWithoutExtraInput should return false after operator is finished",
                operator.canProduceMoreDataWithoutExtraInput()
            );
        }
    }

    /**
     * Extracts and asserts the operator status.
     */
    protected final void assertOperatorStatus(Operator operator, List<Page> input, List<Page> output) {
        Operator.Status status = operator.status();

        if (status == null) {
            assertStatus(null, input, output);
            return;
        }

        var xContent = XContentType.JSON.xContent();
        try (var xContentBuilder = XContentBuilder.builder(xContent)) {
            status.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            var bytesReference = BytesReference.bytes(xContentBuilder);
            var map = XContentHelper.convertToMap(bytesReference, false, xContentBuilder.contentType()).v2();

            assertStatus(map, input, output);
        } catch (IOException e) {
            fail(e, "Failed to convert operator status to XContent");
        }
    }

    /**
     * Assert that the status is sane.
     * <p>
     *     This method should be overridden with custom logics and for better assertions and for operators without status.
     * </p>
     */
    protected void assertStatus(@Nullable Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, notNullValue());

        var totalInputRows = input.stream().mapToInt(Page::getPositionCount).sum();
        var totalOutputRows = output.stream().mapToInt(Page::getPositionCount).sum();

        MapMatcher matcher = matchesMap().extraOk();
        if (map.containsKey("pages_processed")) {
            matcher = matcher.entry("pages_processed", greaterThanOrEqualTo(0));
        } else {
            matcher = matcher.entry("pages_received", input.size()).entry("pages_emitted", output.size());
        }
        matcher = matcher.entry("rows_received", totalInputRows).entry("rows_emitted", totalOutputRows);
        assertMap(map, matcher);
    }

    /**
     * A {@link DriverContext} with a nonBreakingBigArrays.
     */
    protected final DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    protected final DriverContext crankyDriverContext() {
        BlockFactory blockFactory = crankyBlockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }
}
