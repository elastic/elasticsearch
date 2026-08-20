/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class StreamingPageOperatorTests extends AnyOperatorTestCase {

    private static final int PAGE_SIZE = 1024;

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new StreamingPageOperator.Factory(new PageStreamPublisher(PAGE_SIZE), Function.identity());
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("StreamingPageOperator");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("StreamingPageOperator");
    }

    @Override
    public void testSimpleDescription() {
        Operator.OperatorFactory factory = simple();
        assertThat(factory.describe(), expectedDescriptionOfSimple());
        try (Operator op = factory.get(driverContext())) {
            assertThat(op.toString(), expectedToStringOfSimple());
        }
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertNotNull(map);
        assertThat(map.get("pages_emitted"), equalTo(input.size()));
        assertThat(map.get("rows_emitted"), equalTo(input.stream().mapToInt(Page::getPositionCount).sum()));
    }

    public void testInitiallyBlocked() {
        PageStreamPublisher publisher = new PageStreamPublisher(PAGE_SIZE);
        try (StreamingPageOperator operator = new StreamingPageOperator(publisher, publisher.registerProducer(), Function.identity())) {
            assertFalse("operator should not need input before any subscriber demand", operator.needsInput());
            assertFalse("unblock listener should not be done before demand", operator.isBlocked().listener().isDone());
        }
    }

    public void testUnblockedAfterDemand() {
        PageStreamPublisher publisher = new PageStreamPublisher(PAGE_SIZE);
        TestSubscriber subscriber = new TestSubscriber();
        publisher.subscribe(subscriber);
        subscriber.requestOne();

        try (StreamingPageOperator operator = new StreamingPageOperator(publisher, publisher.registerProducer(), Function.identity())) {
            assertTrue("operator should accept input after subscriber demand", operator.needsInput());
            assertTrue("unblock listener should be done after demand", operator.isBlocked().listener().isDone());
        }
    }

    public void testAlignmentApplied() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = new TestSubscriber();
        publisher.subscribe(subscriber);
        subscriber.requestOne();

        Function<Page, Page> sliceToOneRow = page -> {
            Page sliced = page.slice(0, 1);
            page.releaseBlocks();
            return sliced;
        };

        try (StreamingPageOperator operator = new StreamingPageOperator(publisher, publisher.registerProducer(), sliceToOneRow)) {
            long[] values = { 1L, 2L };
            Block block = blockFactory().newLongArrayVector(values, 2).asBlock();
            operator.addInput(new Page(2, block));
            operator.finish();
        }

        assertThat("alignment should have reduced the page to 1 row", subscriber.receivedPages, hasSize(1));
        assertThat(subscriber.receivedPages.get(0).getPositionCount(), equalTo(1));
        for (Page page : subscriber.receivedPages) {
            page.releaseBlocks();
        }
    }

    public void testPageAndRowCounting() {
        PageStreamPublisher publisher = new PageStreamPublisher(10_000);
        TestSubscriber subscriber = new TestSubscriber();
        publisher.subscribe(subscriber);
        subscriber.requestOne();

        try (StreamingPageOperator operator = new StreamingPageOperator(publisher, publisher.registerProducer(), Function.identity())) {
            for (int rowCount : new int[] { 1, 2, 3 }) {
                long[] values = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    values[i] = i;
                }
                Block block = blockFactory().newLongArrayVector(values, rowCount).asBlock();
                operator.addInput(new Page(rowCount, block));
            }

            StreamingPageOperator.Status status = operator.status();
            assertThat(status.pagesEmitted(), equalTo(3));
            assertThat(status.rowsEmitted(), equalTo(6L));
            operator.finish();
        }

        for (Page page : subscriber.receivedPages) {
            page.releaseBlocks();
        }
    }

    public void testFinishSetsIsFinished() {
        PageStreamPublisher publisher = new PageStreamPublisher(PAGE_SIZE);
        try (StreamingPageOperator operator = new StreamingPageOperator(publisher, publisher.registerProducer(), Function.identity())) {
            assertFalse("not finished before finish() is called", operator.isFinished());
            operator.finish();
            assertTrue("finished after finish() is called", operator.isFinished());
        }
    }

    public void testNeedsInputFalseAfterFinish() {
        PageStreamPublisher publisher = new PageStreamPublisher(PAGE_SIZE);
        TestSubscriber subscriber = new TestSubscriber();
        publisher.subscribe(subscriber);
        subscriber.requestOne();

        try (StreamingPageOperator operator = new StreamingPageOperator(publisher, publisher.registerProducer(), Function.identity())) {
            assertTrue("operator should need input when subscriber has demand", operator.needsInput());
            operator.finish();
            assertFalse("operator should not need input after finish()", operator.needsInput());
        }
    }

    private static class TestSubscriber implements Flow.Subscriber<Page> {
        private Flow.Subscription subscription;
        final List<Page> receivedPages = new ArrayList<>();

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.subscription = s;
        }

        @Override
        public void onNext(Page page) {
            receivedPages.add(page);
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onComplete() {}

        void requestOne() {
            subscription.request(1);
        }
    }
}
