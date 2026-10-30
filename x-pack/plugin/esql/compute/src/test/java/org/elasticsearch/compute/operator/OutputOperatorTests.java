/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class OutputOperatorTests extends AnyOperatorTestCase {
    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new OutputOperator.OutputOperatorFactory(List.of("a"), p -> p, p -> {});
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("OutputOperator[columns = [a]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    private Operator.OperatorFactory big() {
        return new OutputOperator.OutputOperatorFactory(IntStream.range(0, 20).mapToObj(i -> "a" + i).toList(), p -> p, p -> {});
    }

    private String expectedDescriptionOfBig() {
        return "OutputOperator[columns = [20 columns]]";
    }

    public void testBigToString() {
        try (Operator operator = big().get(driverContext())) {
            assertThat(operator.toString(), equalTo(expectedDescriptionOfBig()));
        }
    }

    public void testBigDescription() {
        assertThat(big().describe(), equalTo(expectedDescriptionOfBig()));
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, nullValue());
    }

    /** A consumer that records what the operator tells it and can be told to hold the driver back. */
    private static class TrackingConsumer implements PageConsumer {
        DriverContext boundTo;
        final List<Page> pages = new ArrayList<>();
        SubscribableListener<Void> blocked;
        boolean finished;
        boolean done;
        boolean closed;

        @Override
        public void bind(DriverContext driverContext) {
            boundTo = driverContext;
        }

        @Override
        public void accept(Page page) {
            pages.add(page);
        }

        @Override
        public IsBlockedResult isBlocked() {
            return blocked == null ? Operator.NOT_BLOCKED : new IsBlockedResult(blocked, "tracking");
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean isFinished() {
            return done;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    public void testPageConsumerIsBoundToTheDriver() {
        TrackingConsumer consumer = new TrackingConsumer();
        DriverContext driverContext = driverContext();
        try (Operator operator = new OutputOperator.OutputOperatorFactory(List.of("a"), p -> p, consumer).get(driverContext)) {
            assertThat(consumer.boundTo, sameInstance(driverContext));
            assertTrue(operator.needsInput());
        }
        assertTrue(consumer.closed);
    }

    /** The driver only looks at {@code needsInput} while pages flow, so a blocked consumer has to show there. */
    public void testNeedsInputFollowsTheConsumer() {
        TrackingConsumer consumer = new TrackingConsumer();
        try (OutputOperator operator = new OutputOperator(List.of("a"), p -> p, consumer)) {
            assertTrue(operator.needsInput());
            assertThat(operator.isBlocked(), sameInstance(Operator.NOT_BLOCKED));

            consumer.blocked = new SubscribableListener<>();
            assertFalse(operator.needsInput());
            assertThat(operator.isBlocked().listener(), sameInstance(consumer.blocked));

            consumer.blocked.onResponse(null);
            assertTrue(operator.needsInput());
        }
    }

    /** Finishing is handed on to the consumer, and the operator is only finished once the consumer is. */
    public void testFinishWaitsForTheConsumer() {
        TrackingConsumer consumer = new TrackingConsumer();
        try (OutputOperator operator = new OutputOperator(List.of("a"), p -> p, consumer)) {
            assertFalse(operator.isFinished());
            operator.finish();
            assertTrue(consumer.finished);
            assertFalse("the consumer is still working", operator.isFinished());
            assertFalse(operator.needsInput());
            consumer.done = true;
            assertTrue(operator.isFinished());
        }
    }

    /** A plain consumer is not asked about any of this. */
    public void testPlainConsumerFinishesWithTheOperator() {
        try (OutputOperator operator = new OutputOperator(List.of("a"), p -> p, p -> {})) {
            assertTrue(operator.needsInput());
            operator.finish();
            assertTrue(operator.isFinished());
            assertFalse(operator.needsInput());
        }
    }
}
