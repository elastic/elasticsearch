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
import org.elasticsearch.core.Nullable;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

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

    private static class TrackingConsumer implements PageConsumer {
        @Nullable
        SubscribableListener<Void> blocked;
        boolean finishCalled;
        int pendingOps;

        @Override
        public void consume(Page page) {}

        @Override
        public IsBlockedResult isBlocked() {
            return blocked == null ? Operator.NOT_BLOCKED : new IsBlockedResult(blocked, "tracking");
        }

        @Override
        public void finish() {
            finishCalled = true;
        }

        @Override
        public boolean isFinished() {
            return finishCalled && pendingOps == 0;
        }
    }

    public void testNeedsInputFollowsTheConsumer() {
        TrackingConsumer consumer = new TrackingConsumer();
        try (OutputOperator operator = new OutputOperator(List.of("a"), p -> p, consumer)) {
            assertTrue(operator.needsInput());
            assertThat(operator.isBlocked(), equalTo(Operator.NOT_BLOCKED));

            SubscribableListener<Void> listener = new SubscribableListener<>();
            consumer.blocked = listener;
            assertFalse(operator.needsInput());
            assertFalse(operator.isBlocked().listener().isDone());

            listener.onResponse(null);
            assertTrue(operator.isBlocked().listener().isDone());
            assertTrue(operator.needsInput());
        }
    }

    public void testFinishWaitsForTheConsumer() {
        TrackingConsumer consumer = new TrackingConsumer();
        consumer.pendingOps = 1;
        try (OutputOperator operator = new OutputOperator(List.of("a"), p -> p, consumer)) {
            assertFalse(operator.isFinished());
            operator.finish();
            assertTrue(consumer.finishCalled);
            assertFalse(operator.isFinished());
            assertFalse(operator.needsInput());
            consumer.pendingOps = 0;
            assertTrue(operator.isFinished());
        }
    }
}
