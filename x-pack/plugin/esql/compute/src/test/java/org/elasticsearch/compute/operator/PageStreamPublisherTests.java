/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.MockBlockFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PageStreamPublisherTests extends ComputeTestCase {

    public void testConstructorRejectsNonPositivePageSize() {
        int bad = randomIntBetween(Integer.MIN_VALUE, 0);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new PageStreamPublisher(bad));
        assertThat(e.getMessage(), containsString("pageSize must be at least 1"));
    }

    public void testSubscribeDeliversOnSubscribe() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribe(publisher);
        assertThat("onSubscribe must be called with a non-null subscription", subscriber.subscription, notNullValue());
    }

    public void testWaitForWritingInitiallyBlocked() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        assertDriverBlocked(publisher);
    }

    public void testWaitForWritingUnblockedAfterDemand() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        subscribeWithDemand(publisher);
        assertDriverUnblocked(publisher);
    }

    public void testDriverBlockedAfterDelivery() {
        int pageSize = 3;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        assertTrue("addPage must return true when publisher is active", publisher.addPage(makePage(pageSize)));

        expectPage(subscriber, rows(0, pageSize));
        assertDriverBlocked(publisher);

        subscriber.requestOne();
        assertDriverUnblocked(publisher);
    }

    public void testShortBufferUnblocksDriver() {
        int pageSize = 10;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePage(pageSize - 1));

        expectNoPages(subscriber);
        assertDriverUnblocked(publisher);

        finishStream(publisher);
        expectPage(subscriber, rows(0, pageSize - 1));
    }

    public void testDrainExactFastPath() {
        int pageSize = 4;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePageWithValues(0, pageSize));
        expectPage(subscriber, rows(0, pageSize));
    }

    public void testDrainSliceFastPath() {
        int pageSize = 3;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePageWithValues(0, 5));

        expectPage(subscriber, rows(0, pageSize));
        subscriber.requestOne();
        finishStream(publisher);
        expectPage(subscriber, rows(pageSize, 2));
    }

    public void testDrainMergePath() {
        int pageSize = 5;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        publisher.addPage(makePageWithValues(0, 2));
        expectNoPages(subscriber);
        publisher.addPage(makePageWithValues(2, 2));
        expectNoPages(subscriber);
        publisher.addPage(makePageWithValues(4, 1));
        expectPage(subscriber, rows(0, pageSize));
    }

    public void testMergePathPartialConsumptionRemainder() {
        int pageSize = 3;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePageWithValues(0, 2));
        expectNoPages(subscriber);
        publisher.addPage(makePageWithValues(2, 3));

        expectPage(subscriber, rows(0, pageSize));
        subscriber.requestOne();
        finishStream(publisher);
        expectPage(subscriber, rows(pageSize, 2));
    }

    public void testFooter() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        assertThat("footer must be null before completeWithFooter", publisher.footer(), nullValue());
        publisher.pagesFinished();
        publisher.completeWithFooter(42L, List.of("warn1", "warn2"), true);
        PageStreamPublisher.StreamFooter footer = publisher.footer();
        assertThat(footer, notNullValue());
        assertThat(footer.tookMillis(), equalTo(42L));
        assertThat(footer.warnings(), containsInAnyOrder("warn1", "warn2"));
        assertThat(footer.isPartial(), equalTo(true));
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

        expectPage(subscriber, rows(0, pageSize));
        assertFalse("onComplete must not fire before remainder is delivered", subscriber.completed);
        finishStream(publisher);
        subscriber.requestOne();
        expectPage(subscriber, rows(pageSize, 2));
        subscriber.requestOne();
        assertTrue("onComplete must fire after buffer is drained", subscriber.completed);
    }

    public void testFailStreamWithDemandDeliversError() {
        BlockFactory factory = blockFactory();
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePageWithValues(factory, 0, 3));
        Exception cause = new RuntimeException("boom");
        publisher.failStream(cause);

        assertThat(subscriber.error, sameInstance(cause));
        assertThat(subscriber.errorCount, equalTo(1));
        expectNoPages(subscriber);
        assertThat("pump() terminal branch must release the buffered page", factory.breaker().getUsed(), equalTo(0L));
        subscriber.requestOne();
        assertThat("onError must not be re-delivered on subsequent request", subscriber.errorCount, equalTo(1));
    }

    public void testFailStreamWithoutDemandDeliversErrorImmediately() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribe(publisher);
        Exception cause = new RuntimeException("immediate boom");
        publisher.failStream(cause);

        assertThat(subscriber.error, sameInstance(cause));
        assertThat(subscriber.errorCount, equalTo(1));
        subscriber.requestOne();
        assertThat("onError must not be re-delivered on subsequent request", subscriber.errorCount, equalTo(1));
    }

    public void testFailStreamAfterDemandConsumedDeliversOnError() {
        int pageSize = 3;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePageWithValues(0, pageSize));
        expectPage(subscriber, rows(0, pageSize));

        Exception cause = new RuntimeException("compute failed after page delivery");
        publisher.failStream(cause);
        assertThat("onError must be delivered even though pendingDemand is false", subscriber.error, sameInstance(cause));
    }

    public void testDrainFailureTerminalizesPublisher() {
        BigArrays bigArrays = nonBreakingBigArrays();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        ArmableBlockFactory factory = new ArmableBlockFactory(BlockFactory.builder(bigArrays).breaker(breaker));
        try {
            int pageSize = 5;
            PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
            TestSubscriber subscriber = subscribeWithDemand(publisher);
            publisher.addPage(makePageWithValues(factory, 0, 2));
            expectNoPages(subscriber);

            factory.throwOnNextLongBlockBuilder = true;
            CircuitBreakingException thrown = expectThrows(
                CircuitBreakingException.class,
                () -> publisher.addPage(makePageWithValues(factory, 2, 3))
            );
            assertThat("the exception from buildPage must propagate out of addPage", thrown, notNullValue());
            assertThat("subscriber must receive onError from the drain failure", subscriber.error, sameInstance(thrown));
            expectNoPages(subscriber);
            assertDriverUnblocked(publisher);
        } finally {
            factory.ensureAllBlocksAreReleased();
            assertThat("breaker must be back to zero after drain failure", breaker.getUsed(), equalTo(0L));
        }
    }

    public void testFailStreamIdempotent() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        assertThat("failure() must be null before any failStream", publisher.failure(), nullValue());

        Exception first = new RuntimeException("first");
        Exception second = new RuntimeException("second");
        publisher.failStream(first);
        assertThat("failure() must return the first exception", publisher.failure(), sameInstance(first));
        publisher.failStream(second);
        assertThat("only the first error must be delivered", subscriber.error, sameInstance(first));
        assertThat("failure() must still return the first exception after a second failStream", publisher.failure(), sameInstance(first));
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

        assertDriverUnblocked(publisher);
        expectNoPages(subscriber);
    }

    public void testCancelWithPartiallyConsumedFrontPageReleasesBuffer() {
        BlockFactory factory = blockFactory();
        int pageSize = 2;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        publisher.addPage(makePageWithValues(factory, 0, 5));
        expectPage(subscriber, rows(0, pageSize));
        assertThat("remainder must still be breaker-resident before cancel", factory.breaker().getUsed(), greaterThan(0L));

        subscriber.cancel();

        assertThat("cancel must release all buffered rows, returning breaker to zero", factory.breaker().getUsed(), equalTo(0L));
        assertDriverUnblocked(publisher);
        assertFalse("addPage must return false on a cancelled publisher", publisher.addPage(makePageWithValues(factory, 5, 1)));
    }

    public void testCancelSuppressesOnComplete() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.pagesFinished();
        subscriber.cancel();
        publisher.completeWithFooter(0, List.of(), false);

        assertFalse("onComplete must not fire after cancel", subscriber.completed);
        assertThat("no onError must be delivered by cancel", subscriber.error, nullValue());
    }

    public void testAddPageAfterCancelReleasesBlocks() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        subscriber.cancel();
        assertFalse("addPage must return false after cancel", publisher.addPage(makePage(5)));

        expectNoPages(subscriber);
        assertDriverUnblocked(publisher);
    }

    public void testAddPageAfterTerminalStateReleasesBlocks() {
        PageStreamPublisher publisher = new PageStreamPublisher(1024);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.failStream(new RuntimeException("terminal"));
        assertFalse("addPage must return false after terminal state", publisher.addPage(makePage(5)));

        expectNoPages(subscriber);
    }

    public void testPageSizeOne() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribe(publisher);

        int rowCount = 4;
        for (int i = 0; i < rowCount; i++) {
            subscriber.requestOne();
            publisher.addPage(makePageWithValues(i, 1));
        }

        expectPages(subscriber, rows(0, 1), rows(1, 1), rows(2, 1), rows(3, 1));
    }

    public void testMergePathUnderCircuitBreaking() {
        testWithCrankyBlockFactory(cranky -> {
            int pageSize = 5;
            PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
            TestSubscriber subscriber = subscribeWithDemand(publisher);
            try {
                publisher.addPage(makePageWithValues(cranky, 0, 2));
                publisher.addPage(makePageWithValues(cranky, 2, 2));
                publisher.addPage(makePageWithValues(cranky, 4, 1));
                expectPage(subscriber, rows(0, pageSize));
                finishStream(publisher);
            } finally {
                releaseStream(subscriber);
            }
        });
    }

    public void testMergeConstantNullFrontWithValuedPage() {
        int pageSize = 5;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        publisher.addPage(makeNullPage(2));
        expectNoPages(subscriber);
        publisher.addPage(makePageWithValues(0, 3));
        expectPage(subscriber, null, null, 0L, 1L, 2L);
    }

    public void testMergeValuedFrontWithConstantNullPage() {
        int pageSize = 5;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        publisher.addPage(makePageWithValues(0, 3));
        expectNoPages(subscriber);
        publisher.addPage(makeNullPage(2));
        expectPage(subscriber, 0L, 1L, 2L, null, null);
    }

    public void testMergeAllNullPagesDeliverNullBlock() {
        int pageSize = 5;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        publisher.addPage(makeNullPage(2));
        publisher.addPage(makeNullPage(3));

        assertThat("merge of two null pages must produce one chunk", subscriber.receivedPages, hasSize(1));
        assertTrue(
            "delivered block must be all-null when every contributing block is null",
            subscriber.receivedPages.get(0).getBlock(0).areAllValuesNull()
        );
        releasePages(subscriber.receivedPages);
    }

    public void testMergeConstantNullWithPartialConsumptionRemainder() {
        int pageSize = 3;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        publisher.addPage(makeNullPage(2));
        publisher.addPage(makePageWithValues(0, 3));

        expectPage(subscriber, null, null, 0L);
        subscriber.requestOne();
        finishStream(publisher);
        expectPage(subscriber, 1L, 2L);
    }

    public void testMergePicksElementTypePerColumn() {
        int pageSize = 4;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        Block nullBlock1 = blockFactory().newConstantNullBlock(2);
        long[] vals1 = { 0L, 1L };
        Block longBlock1 = blockFactory().newLongArrayVector(vals1, 2).asBlock();
        publisher.addPage(new Page(2, new Block[] { nullBlock1, longBlock1 }));
        expectNoPages(subscriber);

        long[] vals2 = { 10L, 11L };
        Block longBlock2 = blockFactory().newLongArrayVector(vals2, 2).asBlock();
        Block nullBlock2 = blockFactory().newConstantNullBlock(2);
        publisher.addPage(new Page(2, new Block[] { longBlock2, nullBlock2 }));

        assertThat("merge across two pages must produce one 4-row chunk", subscriber.receivedPages, hasSize(1));
        Page chunk = subscriber.receivedPages.get(0);
        assertThat(chunk.getPositionCount(), equalTo(4));
        assertLongsWithNulls(chunk, 0, null, null, 10L, 11L);
        assertLongsWithNulls(chunk, 1, 0L, 1L, null, null);
        releasePages(subscriber.receivedPages);
    }

    public void testMergeConstantNullUnderCircuitBreaking() {
        testWithCrankyBlockFactory(cranky -> {
            int pageSize = 5;
            PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
            TestSubscriber subscriber = subscribeWithDemand(publisher);
            try {
                publisher.addPage(makeNullPage(cranky, 2));
                publisher.addPage(makePageWithValues(cranky, 0, 3));
                expectPage(subscriber, null, null, 0L, 1L, 2L);
                finishStream(publisher);
            } finally {
                releaseStream(subscriber);
            }
        });
    }

    public void testMergeAfterPartialConsumptionOfConstantNullFront() {
        int pageSize = 4;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        publisher.addPage(makeNullPage(5));
        expectPage(subscriber, null, null, null, null);

        subscriber.requestOne();
        publisher.addPage(makePageWithValues(0, 3));
        expectPage(subscriber, null, 0L, 1L, 2L);
    }

    public void testMergeAfterPartialConsumptionOfTypedFrontWithNullTail() {
        int pageSize = 4;
        PageStreamPublisher publisher = new PageStreamPublisher(pageSize);
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        publisher.addPage(makePageWithValues(0, 5));
        expectPage(subscriber, rows(0, 4));

        subscriber.requestOne();
        publisher.addPage(makeNullPage(3));
        expectPage(subscriber, 4L, null, null, null);
    }

    public void testSingleLargePageDrainedOneRowAtATime() {
        int rowCount = 8;
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.addPage(makePageWithValues(0, rowCount));

        for (int i = 0; i < rowCount; i++) {
            expectPage(subscriber, rows(i, 1));
            if (i < rowCount - 1) {
                subscriber.requestOne();
            }
        }

        subscriber.requestOne();
        finishStream(publisher);
        assertTrue("stream must complete after all rows delivered", subscriber.completed);
    }

    public void testSurplusDemandIsHonoured() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.requestN(2);
        assertTwoCreditsThenThird(publisher, subscriber);
    }

    public void testAccumulatedSingleRequestsAreHonoured() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.requestOne();
        subscriber.requestOne();
        assertTwoCreditsThenThird(publisher, subscriber);
    }

    public void testSurplusDemandUnblocksDriverWhenBufferRunsShort() {
        PageStreamPublisher publisher = new PageStreamPublisher(2);
        TestSubscriber subscriber = subscribe(publisher);

        subscriber.requestN(2);
        publisher.addPage(makePageWithValues(0, 3));

        expectPage(subscriber, rows(0, 2));
        assertDriverUnblocked(publisher);
        publisher.addPage(makePageWithValues(3, 1));
        expectPage(subscriber, rows(2, 2));
    }

    public void testDemandSaturatesAtMaxValue() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribe(publisher);

        subscriber.requestN(Long.MAX_VALUE);
        subscriber.requestN(Long.MAX_VALUE);
        int pageCount = 5;
        for (int i = 0; i < pageCount; i++) {
            publisher.addPage(makePageWithValues(i, 1));
        }
        expectPages(subscriber, rows(0, 1), rows(1, 1), rows(2, 1), rows(3, 1), rows(4, 1));
    }

    public void testRequestNonPositiveDeliversOnError() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribe(publisher);

        long n = randomBoolean() ? 0L : -randomLongBetween(1, 1024);
        subscriber.requestN(n);

        assertThat(subscriber.error, notNullValue());
        assertThat(subscriber.error.getClass().getName(), equalTo(IllegalArgumentException.class.getName()));
        assertThat(subscriber.errorCount, equalTo(1));
        expectNoPages(subscriber);
    }

    public void testCancelFromInsideOnNextStopsDelivery() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.requestN(3);
        subscriber.onFirstPage = () -> subscriber.cancel();
        publisher.addPage(makePageWithValues(0, 3));

        expectPage(subscriber, rows(0, 1));
        assertDriverUnblocked(publisher);
    }

    public void testFailStreamFromInsideOnNextDeliversOnErrorOnce() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.requestN(3);
        Exception cause = new RuntimeException("re-entrant failure");
        subscriber.onFirstPage = () -> publisher.failStream(cause);
        publisher.addPage(makePageWithValues(0, 3));

        expectPage(subscriber, rows(0, 1));
        assertThat(subscriber.errorCount, equalTo(1));
        assertThat(subscriber.error, sameInstance(cause));
        assertFalse("onComplete must not fire after failStream", subscriber.completed);
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

    private void assertTwoCreditsThenThird(PageStreamPublisher publisher, TestSubscriber subscriber) {
        publisher.addPage(makePageWithValues(0, 3));
        expectPages(subscriber, rows(0, 1), rows(1, 1));
        subscriber.requestOne();
        expectPage(subscriber, rows(2, 1));
    }

    private static void expectPages(TestSubscriber subscriber, Long[]... pages) {
        assertThat(subscriber.receivedPages, hasSize(pages.length));
        for (int i = 0; i < pages.length; i++) {
            assertLongsWithNulls(subscriber.receivedPages.get(i), 0, pages[i]);
        }
        releasePages(subscriber.receivedPages);
    }

    private static void expectPage(TestSubscriber subscriber, Long... column0) {
        expectPages(subscriber, column0);
    }

    private static void expectNoPages(TestSubscriber subscriber) {
        assertThat(subscriber.receivedPages, hasSize(0));
    }

    private static Long[] rows(long startValue, int count) {
        Long[] result = new Long[count];
        for (int i = 0; i < count; i++) {
            result[i] = startValue + i;
        }
        return result;
    }

    private static void assertDriverBlocked(PageStreamPublisher publisher) {
        assertFalse(publisher.waitForWriting().listener().isDone());
    }

    private static void assertDriverUnblocked(PageStreamPublisher publisher) {
        assertTrue(publisher.waitForWriting().listener().isDone());
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

    private Page makeNullPage(int rows) {
        return makeNullPage(blockFactory(), rows);
    }

    private static Page makeNullPage(BlockFactory factory, int rows) {
        return new Page(rows, new Block[] { factory.newConstantNullBlock(rows) });
    }

    private static void assertLongsWithNulls(Page page, int channel, Long... expected) {
        assertThat(page.getPositionCount(), equalTo(expected.length));
        LongBlock block = page.getBlock(channel);
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] == null) {
                assertTrue("expected null at position " + i, block.isNull(i));
            } else {
                assertFalse("expected non-null at position " + i, block.isNull(i));
                assertThat("value at position " + i, block.getLong(block.getFirstValueIndex(i)), equalTo(expected[i]));
            }
        }
    }

    private static void releasePages(List<Page> pages) {
        for (Page page : pages) {
            page.releaseBlocks();
        }
        pages.clear();
    }

    private static void releaseStream(TestSubscriber subscriber) {
        subscriber.cancel();
        releasePages(subscriber.receivedPages);
    }

    public void testSynchronousRequestFromInsideOnNext() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.onFirstPage = subscriber::requestOne;
        subscriber.requestOne();

        publisher.addPage(makePageWithValues(0, 3));
        assertThat(subscriber.receivedPages, hasSize(2));

        subscriber.requestOne();
        finishStream(publisher);
        expectPages(subscriber, rows(0, 1), rows(1, 1), rows(2, 1));
    }

    public void testReentrantAddPageFromUnblockListener() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        TestSubscriber subscriber = subscribe(publisher);
        Page page = makePageWithValues(0, 1);
        publisher.waitForWriting().listener().addListener(ActionListener.running(() -> publisher.addPage(page)));

        subscriber.requestOne();
        assertThat(subscriber.receivedPages, hasSize(1));
        assertThat(subscriber.error, nullValue());
        releasePages(subscriber.receivedPages);
    }

    public void testMonitorNotHeldDuringSubscriberCallbacks() {
        PageStreamPublisher publisher = new PageStreamPublisher(1);
        List<String> violations = new ArrayList<>();

        Flow.Subscriber<Page> lockCheckingSubscriber = new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription s) {
                this.subscription = s;
                s.request(1);
            }

            @Override
            public void onNext(Page page) {
                if (Thread.holdsLock(publisher)) {
                    violations.add("publisher lock held during onNext");
                }
                page.releaseBlocks();
                subscription.request(1);
            }

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onComplete() {
                if (Thread.holdsLock(publisher)) {
                    violations.add("publisher lock held during onComplete");
                }
            }
        };

        publisher.subscribe(lockCheckingSubscriber);
        publisher.addPage(makePageWithValues(0, 1));
        publisher.addPage(makePageWithValues(1, 1));
        publisher.addPage(makePageWithValues(2, 1));
        finishStream(publisher);

        assertThat("subscriber callbacks must not hold the publisher monitor", violations, hasSize(0));
    }

    public void testCancellationDuringBuildPageReleasesBlocks() {
        PageStreamPublisher publisher = new PageStreamPublisher(5);
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.requestOne();
        subscriber.onFirstPage = subscriber::cancel;

        publisher.addPage(makePageWithValues(0, 3));
        publisher.addPage(makePageWithValues(3, 3));
        finishStream(publisher);

        releasePages(subscriber.receivedPages);
    }

    private static class ArmableBlockFactory extends MockBlockFactory {
        volatile boolean throwOnNextLongBlockBuilder;

        ArmableBlockFactory(BlockFactoryBuilder builder) {
            super(builder);
        }

        @Override
        public LongBlock.Builder newLongBlockBuilder(int estimatedSize) {
            if (throwOnNextLongBlockBuilder) {
                throwOnNextLongBlockBuilder = false;
                throw new CircuitBreakingException("test drain failure", CircuitBreaker.Durability.PERMANENT);
            }
            return super.newLongBlockBuilder(estimatedSize);
        }
    }

    private static class TestSubscriber implements Flow.Subscriber<Page> {
        private Flow.Subscription subscription;
        final List<Page> receivedPages = new ArrayList<>();
        Throwable error;
        int errorCount;
        boolean completed;
        Runnable onFirstPage;

        @Override
        public void onSubscribe(Flow.Subscription s) {
            this.subscription = s;
        }

        @Override
        public void onNext(Page page) {
            receivedPages.add(page);
            Runnable hook = onFirstPage;
            if (hook != null) {
                onFirstPage = null;
                hook.run();
            }
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
            this.errorCount++;
        }

        @Override
        public void onComplete() {
            this.completed = true;
        }

        void requestOne() {
            subscription.request(1);
        }

        void requestN(long n) {
            subscription.request(n);
        }

        void cancel() {
            subscription.cancel();
        }
    }
}
