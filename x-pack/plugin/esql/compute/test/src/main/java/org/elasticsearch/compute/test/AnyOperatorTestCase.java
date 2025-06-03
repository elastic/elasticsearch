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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;

import java.io.IOException;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.hasKey;
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
     * Ensures that the Operator.Status of this operator has the standard fields.
     */
    public void testOperatorStatus() throws IOException {
        DriverContext driverContext = driverContext();
        try (var operator = simple().get(driverContext)) {
            Operator.Status status = operator.status();

            assumeTrue("Operator does not provide a status", status != null);

            var xContent = XContentType.JSON.xContent();
            try (var xContentBuilder = XContentBuilder.builder(xContent)) {
                status.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

                var bytesReference = BytesReference.bytes(xContentBuilder);
                var map = XContentHelper.convertToMap(bytesReference, false, xContentBuilder.contentType()).v2();

                if (operator instanceof SourceOperator) {
                    assertThat(map, hasKey("pages_emitted"));
                    assertThat(map, hasKey("rows_emitted"));
                } else if (operator instanceof SinkOperator) {
                    assertThat(map, hasKey("pages_received"));
                    assertThat(map, hasKey("rows_received"));
                } else {
                    assertThat(map, either(hasKey("pages_processed")).or(both(hasKey("pages_received")).and(hasKey("pages_emitted"))));
                    assertThat(map, hasKey("rows_received"));
                    assertThat(map, hasKey("rows_emitted"));
                }
            }
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
