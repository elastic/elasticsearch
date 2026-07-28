/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PageStreamPublisherTests extends ComputeTestCase {

    public void testSubscribeDeliversOnSubscribe() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribe(publisher);
        assertThat("onSubscribe must be called with a non-null subscription", subscriber.subscription, notNullValue());
    }

    public void testWaitForWritingInitiallyBlocked() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        assertFalse("unblock listener must not be done before any subscriber demand", publisher.waitForWriting().listener().isDone());
    }

    public void testWaitForWritingUnblockedAfterDemand() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        subscribeWithDemand(publisher);
        assertTrue("unblock listener must be done after request(1)", publisher.waitForWriting().listener().isDone());
    }

    public void testDriverBlockedAfterDelivery() {
        int pageSize = 3;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePage(pageSize));

        assertThat("subscriber should have received one page", subscriber.receivedPages, hasSize(1));
        assertFalse("driver must be blocked again after delivery", publisher.waitForWriting().listener().isDone());

        subscriber.requestOne();
        assertTrue("driver must unblock on next request", publisher.waitForWriting().listener().isDone());

        releasePages(subscriber.receivedPages);
    }

    public void testShortBufferUnblocksDriver() {
        int pageSize = 10;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePage(pageSize - 1));

        assertThat("no page should be delivered while buffer is short", subscriber.receivedPages, hasSize(0));
        assertTrue("driver must be unblocked to produce more rows", publisher.waitForWriting().listener().isDone());

        finishStream(publisher);
        assertThat("short remainder should be flushed on pagesFinished", subscriber.receivedPages, hasSize(1));
        releasePages(subscriber.receivedPages);
    }

    public void testDrainExactFastPath() {
        int pageSize = 4;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePageWithValues(0, pageSize));

        assertSinglePage(subscriber, 0, pageSize);
        releasePages(subscriber.receivedPages);
    }

    public void testDrainSliceFastPath() {
        int pageSize = 3;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePageWithValues(0, 5));

        assertSinglePage(subscriber, 0, pageSize);
        releasePages(subscriber.receivedPages);

        subscriber.requestOne();
        finishStream(publisher);
        assertSinglePage(subscriber, pageSize, 2);
        assertTrue("onComplete should fire after footer + pagesFinished", subscriber.completed);
        releasePages(subscriber.receivedPages);
    }

    public void testDrainMergePath() {
        int pageSize = 5;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        publisher.addPage(makePageWithValues(0, 2));
        assertThat("no page should be delivered while buffer is short", subscriber.receivedPages, hasSize(0));
        publisher.addPage(makePageWithValues(2, 2));
        assertThat("no page should be delivered while buffer is short", subscriber.receivedPages, hasSize(0));
        publisher.addPage(makePageWithValues(4, 1));

        assertSinglePage(subscriber, 0, pageSize);
        releasePages(subscriber.receivedPages);
    }

    public void testMergePathPartialConsumptionRemainder() {
        int pageSize = 3;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePageWithValues(0, 2));
        assertThat("no page should be delivered while buffer is short", subscriber.receivedPages, hasSize(0));
        publisher.addPage(makePageWithValues(2, 3));

        assertSinglePage(subscriber, 0, pageSize);
        releasePages(subscriber.receivedPages);
        subscriber.requestOne();
        finishStream(publisher);

        assertSinglePage(subscriber, pageSize, 2);
        releasePages(subscriber.receivedPages);
    }

    public void testGetFooter() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        assertThat("footer must be null before completeWithFooter", publisher.getFooter(), nullValue());
        publisher.pagesFinished();
        publisher.completeWithFooter(42L, List.of("warn1", "warn2"), true);
        PageStreamPublisher.StreamFooter footer = publisher.getFooter();
        assertThat(footer, notNullValue());
        assertThat(footer.tookMillis(), equalTo(42L));
        assertThat(footer.warnings(), containsInAnyOrder("warn1", "warn2"));
        assertThat(footer.isPartial(), equalTo(true));
    }

    public void testIsPageStreamFinished() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        assertFalse("should not be finished before pagesFinished()", publisher.isPageStreamFinished());
        publisher.pagesFinished();
        assertTrue("should be finished after pagesFinished()", publisher.isPageStreamFinished());
    }

    public void testShortRemainderFlushedOnPagesFinished() {
        int pageSize = 10;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePage(3));
        publisher.pagesFinished();

        assertSinglePage(subscriber, 0, 3);
        releasePages(subscriber.receivedPages);
    }

    public void testCompletionRequiresFooterAndDemand_footerFirst() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribe(publisher);

        publisher.pagesFinished();
        publisher.completeWithFooter(1L, List.of(), false);
        assertFalse("onComplete must not fire without demand", subscriber.completed);
        subscriber.requestOne();
        assertTrue("onComplete must fire once demand arrives", subscriber.completed);
    }

    public void testCompletionRequiresFooterAndDemand_demandFirst() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.pagesFinished();

        assertFalse("onComplete must not fire without footer", subscriber.completed);
        publisher.completeWithFooter(1L, List.of(), false);
        assertTrue("onComplete must fire once footer arrives", subscriber.completed);
    }

    public void testCompletionOrdering() {
        int pageSize = 3;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePageWithValues(0, 5));

        assertSinglePage(subscriber, 0, pageSize);
        assertFalse("onComplete must not fire before remainder is delivered", subscriber.completed);
        releasePages(subscriber.receivedPages);
        finishStream(publisher);
        subscriber.requestOne();
        assertSinglePage(subscriber, pageSize, 2);
        releasePages(subscriber.receivedPages);
        subscriber.requestOne();
        assertTrue("onComplete must fire after buffer is drained", subscriber.completed);
    }

    public void testFailStreamWithDemandDeliversError() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePage(3));
        Exception cause = new RuntimeException("boom");
        publisher.failStream(cause);

        assertThat("onError must fire immediately when demand is present", subscriber.error, sameInstance(cause));
        assertThat("no onNext must be emitted after failure", subscriber.receivedPages, hasSize(0));
    }

    public void testFailStreamWithoutDemandDeliversErrorOnNextRequest() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribe(publisher);
        Exception cause = new RuntimeException("deferred boom");
        publisher.failStream(cause);

        assertThat("error must not be delivered without demand", subscriber.error, nullValue());
        subscriber.requestOne();
        assertThat("error must be delivered on next request", subscriber.error, sameInstance(cause));
    }

    public void testFailStreamIdempotent() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        Exception first = new RuntimeException("first");
        Exception second = new RuntimeException("second");
        publisher.failStream(first);
        publisher.failStream(second);
        assertThat("only the first error must be delivered", subscriber.error, sameInstance(first));
    }

    public void testCompleteWithFooterAfterFailStreamDoesNotEmitOnComplete() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.pagesFinished();
        publisher.failStream(new RuntimeException("fail"));
        publisher.completeWithFooter(0, List.of(), false);

        assertThat("error must be delivered", subscriber.error, notNullValue());
        assertFalse("onComplete must not fire after failStream", subscriber.completed);
    }

    public void testCancelDrainsBufferAndUnblocksDriver() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePage(3));
        subscriber.cancel();

        assertTrue("driver must be unblocked after cancel", publisher.waitForWriting().listener().isDone());
        assertThat("no page should have been delivered", subscriber.receivedPages, hasSize(0));
    }

    public void testAddPageAfterCancelReleasesBlocks() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        subscriber.cancel();
        publisher.addPage(makePage(5));

        assertThat("no page delivered after cancel", subscriber.receivedPages, hasSize(0));
        assertTrue("driver must remain unblocked", publisher.waitForWriting().listener().isDone());
    }

    public void testAddPageAfterTerminalStateReleasesBlocks() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.failStream(new RuntimeException("terminal"));
        publisher.addPage(makePage(5));

        assertThat("no additional onNext after terminal state", subscriber.receivedPages, hasSize(0));
    }

    public void testPageSizeOne() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribe(publisher);

        int rowCount = 4;
        for (int i = 0; i < rowCount; i++) {
            subscriber.requestOne();
            publisher.addPage(makePageWithValues(i, 1));
        }

        assertThat("each row must be delivered as a separate page", subscriber.receivedPages, hasSize(rowCount));
        for (int i = 0; i < rowCount; i++) {
            assertThat(subscriber.receivedPages.get(i).getPositionCount(), equalTo(1));
            assertPageValues(subscriber.receivedPages.get(i), i, 1);
        }
        releasePages(subscriber.receivedPages);
    }

    public void testMergePathUnderCircuitBreaking() {
        testWithCrankyBlockFactory(cranky -> {
            int pageSize = 5;
            PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
            TestSubscriber subscriber = subscribeWithDemand(publisher);
            publisher.addPage(makePageWithValues(cranky, 0, 2));
            publisher.addPage(makePageWithValues(cranky, 2, 2));
            publisher.addPage(makePageWithValues(cranky, 4, 1));

            releasePages(subscriber.receivedPages);
            finishStream(publisher);
            releasePages(subscriber.receivedPages);
        });
    }

    private static TestSubscriber subscribe(PageStreamPublisher publisher) {
        TestSubscriber subscriber = new TestSubscriber();
        publisher.subscribe(subscriber);
        return subscriber;
    }

    private static TestSubscriber subscribeWithDemand(PageStreamPublisher publisher) {
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.requestOne();
        return subscriber;
    }

    private static void finishStream(PageStreamPublisher publisher) {
        publisher.pagesFinished();
        publisher.completeWithFooter(0, List.of(), false);
    }

    private static void assertSinglePage(TestSubscriber subscriber, long startValue, int count) {
        assertThat(subscriber.receivedPages, hasSize(1));
        assertPageValues(subscriber.receivedPages.get(0), startValue, count);
    }

    private Page makePage(int rows) {
        return makePageWithValues(0, rows);
    }

    private Page makePageWithValues(long startValue, int count) {
        return makePageWithValues(blockFactory(), startValue, count);
    }

    private static Page makePageWithValues(BlockFactory factory, long startValue, int count) {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = startValue + i;
        }
        return new Page(count, new Block[] { factory.newLongArrayVector(values, count).asBlock() });
    }

    private static void assertPageValues(Page page, long startValue, int count) {
        assertThat(page.getPositionCount(), equalTo(count));
        LongVector vector = page.<LongBlock>getBlock(0).asVector();
        for (int i = 0; i < count; i++) {
            assertThat("row " + i + " value", vector.getLong(i), equalTo(startValue + i));
        }
    }

    private static void releasePages(List<Page> pages) {
        for (Page page : pages) {
            page.releaseBlocks();
        }
        pages.clear();
    }

    private static class TestSubscriber implements Flow.Subscriber<Page> {
        private Flow.Subscription subscription;
        final List<Page> receivedPages = new ArrayList<>();
        Throwable error;
        boolean completed;

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.subscription = s;
        }

        @Override
        public void onNext(Page page) {
            receivedPages.add(page);
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onComplete() {
            this.completed = true;
        }

        void requestOne() {
            subscription.request(1);
        }

        void cancel() {
            subscription.cancel();
        }
    }
}
