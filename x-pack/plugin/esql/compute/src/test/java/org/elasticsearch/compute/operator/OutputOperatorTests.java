/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.spill.SpillStatus;
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

    /** A consumer that writes pages out: every row it is handed counts as spilled. */
    private static class SpillingConsumer extends TrackingConsumer implements OutputOperator.SpillingPageConsumer {
        long rows;

        @Override
        public void consume(Page page) {
            rows += page.getPositionCount();
            page.releaseBlocks();
        }

        @Override
        public long rowsSpilled() {
            return rows;
        }

        @Override
        public SpillStatus spillStatus() {
            return new SpillStatus(rows * Integer.BYTES, 0, 1, 0);
        }
    }

    /**
     * A consumer that writes pages out reaches the operator through its factory as the consumer it is: the operator
     * forwards its blocked state and reports what it received and what the consumer spilled.
     */
    public void testSpillingConsumerBlocksAndReports() {
        SpillingConsumer consumer = new SpillingConsumer();
        PageConsumer.Factory factory = driverContext -> consumer;
        DriverContext driverContext = driverContext();
        try (
            OutputOperator operator = (OutputOperator) new OutputOperator.OutputOperatorFactory(List.of("a"), p -> p, factory).get(
                driverContext
            )
        ) {
            int pages = between(1, 5);
            long rows = 0;
            for (int p = 0; p < pages; p++) {
                int positions = between(1, 100);
                rows += positions;
                operator.addInput(new Page(driverContext.blockFactory().newConstantIntBlockWith(p, positions)));
            }
            OutputOperator.Status status = operator.status();
            assertThat(status.pagesReceived(), equalTo(pages));
            assertThat(status.rowsReceived(), equalTo(rows));
            assertThat(status.rowsSpilled(), equalTo(rows));
            assertThat(status.spill(), equalTo(new SpillStatus(rows * Integer.BYTES, 0, 1, 0)));

            SubscribableListener<Void> listener = new SubscribableListener<>();
            consumer.blocked = listener;
            assertFalse(operator.needsInput());
            assertFalse(operator.isBlocked().listener().isDone());
            listener.onResponse(null);
            assertTrue(operator.needsInput());
        }
    }

    /**
     * A plain consumer is wrapped on its way to the operator, so the operator has no spill to report for it: the
     * reason a consumer that spills goes in through a factory.
     */
    public void testPlainConsumerReportsNoStatus() {
        DriverContext driverContext = driverContext();
        try (Operator operator = new OutputOperator.OutputOperatorFactory(List.of("a"), p -> p, Page::releaseBlocks).get(driverContext)) {
            operator.addInput(new Page(driverContext.blockFactory().newConstantIntBlockWith(1, 3)));
            assertThat(operator.status(), nullValue());
        }
    }
}
