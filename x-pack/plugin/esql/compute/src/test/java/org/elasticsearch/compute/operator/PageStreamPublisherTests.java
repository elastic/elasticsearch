/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PageStreamPublisherTests extends ComputeTestCase {

    private final List<TestSubscriber> allSubscribers = new ArrayList<>();

    @After
    public void checkNoLockViolations() {
        for (TestSubscriber s : allSubscribers) {
            assertThat("publisher monitor must not be held during subscriber callbacks", s.lockViolations, hasSize(0));
        }
        allSubscribers.clear();
    }

    public void testDriverBlockedAfterDelivery() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        assertDriverBlocked(publisher);
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        assertDriverUnblocked(publisher);
        assertTrue("addPage must return true when publisher is active", producer.addPage(makePageWithValues(0, 3)));

        expectPage(subscriber, rows(0, 3));
        assertDriverBlocked(publisher);

        subscriber.requestOne();
        assertDriverUnblocked(publisher);
    }

    public void testSmallPageIsDeliveredWithoutWaitingForMoreRows() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        producer.addPage(makePageWithValues(0, 1));

        expectPage(subscriber, rows(0, 1));
    }

    public void testPagesAreForwardedWholeAndInOrder() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.requestN(2);
        Page first = makePageWithValues(0, 2);
        Page second = makePageWithValues(2, 3);
        producer.addPage(first);
        producer.addPage(second);

        assertThat(subscriber.receivedPages, hasSize(2));
        assertThat("pages must be forwarded as produced, not copied", subscriber.receivedPages.get(0), sameInstance(first));
        assertThat("pages must be forwarded as produced, not copied", subscriber.receivedPages.get(1), sameInstance(second));
        expectPages(subscriber, rows(0, 2), rows(2, 3));
    }

    public void testConstantNullPageIsForwardedAsIs() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        producer.addPage(makeNullPage(3));

        expectPage(subscriber, null, null, null);
    }

    public void testLargePageIsDeliveredAsOnePage() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        producer.addPage(makePageWithValues(0, 8));

        expectPage(subscriber, rows(0, 8));

        subscriber.requestOne();
        finishStream(producer, publisher);
        assertTrue("stream must complete after the page was delivered", subscriber.completed);
    }

    public void testFooter() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        assertThat(publisher.footer(), nullValue());
        producer.finish();
        publisher.completeWithFooter(42L, List.of("warn1", "warn2"), true);
        PageStreamPublisher.StreamFooter footer = publisher.footer();
        assertThat(footer, notNullValue());
        assertThat(footer.tookMillis(), equalTo(42L));
        assertThat(footer.warnings(), containsInAnyOrder("warn1", "warn2"));
        assertThat(footer.isPartial(), equalTo(true));
    }

    public void testCompletionRequiresFooterAndDemand() {
        {
            PageStreamPublisher p = new PageStreamPublisher();
            PageStreamPublisher.Producer prod = p.registerProducer();
            TestSubscriber s = subscribe(p);
            prod.finish();
            p.completeWithFooter(1L, List.of(), false);
            assertFalse("onComplete must not fire without demand", s.completed);
            s.requestOne();
            assertTrue("onComplete must fire once demand arrives", s.completed);
        }
        {
            PageStreamPublisher p = new PageStreamPublisher();
            PageStreamPublisher.Producer prod = p.registerProducer();
            TestSubscriber s = subscribeWithDemand(p);
            prod.finish();
            assertFalse("onComplete must not fire without footer", s.completed);
            p.completeWithFooter(1L, List.of(), false);
            assertTrue("onComplete must fire once footer arrives", s.completed);
        }
    }

    public void testCompletionOrdering() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        producer.addPage(makePageWithValues(0, 3));
        producer.addPage(makePageWithValues(3, 2));

        expectPage(subscriber, rows(0, 3));
        assertFalse("onComplete must not fire before the buffered page is delivered", subscriber.completed);
        finishStream(producer, publisher);
        subscriber.requestOne();
        expectPage(subscriber, rows(3, 2));
        subscriber.requestOne();
        assertTrue("onComplete must fire after buffer is drained", subscriber.completed);
    }

    public void testFailStreamDeliversErrorExactlyOnce() {
        {
            BlockFactory factory = blockFactory();
            PageStreamPublisher p = new PageStreamPublisher();
            PageStreamPublisher.Producer prod = p.registerProducer();
            TestSubscriber s = subscribe(p);
            prod.addPage(makePageWithValues(factory, 0, 3));
            Exception cause = new RuntimeException("boom");
            p.failStream(cause);
            assertThat(s.error, sameInstance(cause));
            assertThat(s.errorCount, equalTo(1));
            expectNoPages(s);
            assertThat("pump terminal branch must release buffered page", factory.breaker().getUsed(), equalTo(0L));
            s.requestOne();
            assertThat(s.errorCount, equalTo(1));
        }
        {
            PageStreamPublisher p = new PageStreamPublisher();
            TestSubscriber s = subscribe(p);
            Exception cause = new RuntimeException("immediate boom");
            p.failStream(cause);
            assertThat(s.error, sameInstance(cause));
            assertThat(s.errorCount, equalTo(1));
            s.requestOne();
            assertThat(s.errorCount, equalTo(1));
        }
        {
            PageStreamPublisher p = new PageStreamPublisher();
            PageStreamPublisher.Producer prod = p.registerProducer();
            TestSubscriber s = subscribeWithDemand(p);
            prod.addPage(makePageWithValues(0, 3));
            expectPage(s, rows(0, 3));
            Exception cause = new RuntimeException("compute failed after page delivery");
            p.failStream(cause);
            assertThat(s.error, sameInstance(cause));
        }
    }

    public void testFailStreamIdempotent() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        assertThat(publisher.failure(), nullValue());

        Exception first = new RuntimeException("first");
        Exception second = new RuntimeException("second");
        publisher.failStream(first);
        assertThat(publisher.failure(), sameInstance(first));
        publisher.failStream(second);
        assertThat(subscriber.error, sameInstance(first));
        assertThat(publisher.failure(), sameInstance(first));
    }

    public void testCompleteWithFooterAfterFailStreamDoesNotEmitOnComplete() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        producer.finish();
        publisher.failStream(new RuntimeException("fail"));
        publisher.completeWithFooter(0, List.of(), false);

        assertThat(subscriber.error, notNullValue());
        assertFalse("onComplete must not fire after failStream", subscriber.completed);
    }

    public void testCancelReleasesBufferedPages() {
        BlockFactory factory = blockFactory();
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        producer.addPage(makePageWithValues(factory, 0, 2));
        producer.addPage(makePageWithValues(factory, 2, 2));
        producer.addPage(makePageWithValues(factory, 4, 2));
        expectPage(subscriber, rows(0, 2));
        assertThat("buffered pages must still be breaker-resident before cancel", factory.breaker().getUsed(), greaterThan(0L));

        subscriber.cancel();

        assertThat("cancel must release all buffered pages, returning breaker to zero", factory.breaker().getUsed(), equalTo(0L));
        assertDriverUnblocked(publisher);
        assertFalse("addPage must return false on a cancelled publisher", producer.addPage(makePageWithValues(factory, 6, 1)));
    }

    public void testCancelSuppressesOnComplete() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        producer.finish();
        subscriber.cancel();
        publisher.completeWithFooter(0, List.of(), false);

        assertFalse("onComplete must not fire after cancel", subscriber.completed);
        assertThat(subscriber.error, nullValue());
    }

    public void testAddPageAfterTerminalStateReleasesBlocks() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);
        publisher.failStream(new RuntimeException("terminal"));
        assertFalse("addPage must return false after terminal state", producer.addPage(makePageWithValues(0, 5)));

        expectNoPages(subscriber);
    }

    public void testDemandAccumulationHonoursCredits() {
        {
            PageStreamPublisher p = new PageStreamPublisher();
            PageStreamPublisher.Producer prod = p.registerProducer();
            TestSubscriber s = subscribe(p);
            s.requestN(2);
            prod.addPage(makePageWithValues(0, 1));
            prod.addPage(makePageWithValues(1, 1));
            prod.addPage(makePageWithValues(2, 1));
            expectPages(s, rows(0, 1), rows(1, 1));
            s.requestOne();
            expectPage(s, rows(2, 1));
        }
        {
            PageStreamPublisher p = new PageStreamPublisher();
            PageStreamPublisher.Producer prod = p.registerProducer();
            TestSubscriber s = subscribe(p);
            s.requestOne();
            s.requestOne();
            prod.addPage(makePageWithValues(0, 1));
            prod.addPage(makePageWithValues(1, 1));
            prod.addPage(makePageWithValues(2, 1));
            expectPages(s, rows(0, 1), rows(1, 1));
            s.requestOne();
            expectPage(s, rows(2, 1));
        }
    }

    public void testSurplusDemandKeepsDriverUnblocked() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribe(publisher);

        subscriber.requestN(2);
        producer.addPage(makePageWithValues(0, 3));

        expectPage(subscriber, rows(0, 3));
        assertDriverUnblocked(publisher);
        producer.addPage(makePageWithValues(3, 1));
        expectPage(subscriber, rows(3, 1));
        assertDriverBlocked(publisher);
    }

    public void testDemandSaturatesAtMaxValue() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribe(publisher);

        subscriber.requestN(Long.MAX_VALUE);
        subscriber.requestN(Long.MAX_VALUE);
        int pageCount = 5;
        for (int i = 0; i < pageCount; i++) {
            producer.addPage(makePageWithValues(i, 1));
        }
        expectPages(subscriber, rows(0, 1), rows(1, 1), rows(2, 1), rows(3, 1), rows(4, 1));
    }

    public void testRequestNonPositiveDeliversOnError() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        TestSubscriber subscriber = subscribe(publisher);

        long n = randomBoolean() ? 0L : -randomLongBetween(1, 1024);
        subscriber.requestN(n);

        assertThat(subscriber.error, instanceOf(IllegalArgumentException.class));
        assertThat(subscriber.errorCount, equalTo(1));
        expectNoPages(subscriber);
    }

    public void testCancelFromInsideOnNextStopsDelivery() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.onFirstPage = subscriber::cancel;
        producer.addPage(makePageWithValues(0, 1));
        producer.addPage(makePageWithValues(1, 1));
        producer.addPage(makePageWithValues(2, 1));
        subscriber.requestN(3);

        expectPage(subscriber, rows(0, 1));
        assertDriverUnblocked(publisher);
    }

    public void testFailStreamFromInsideOnNextDeliversOnErrorOnce() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribe(publisher);
        Exception cause = new RuntimeException("re-entrant failure");
        subscriber.onFirstPage = () -> publisher.failStream(cause);
        producer.addPage(makePageWithValues(0, 1));
        producer.addPage(makePageWithValues(1, 1));
        producer.addPage(makePageWithValues(2, 1));
        subscriber.requestN(3);

        expectPage(subscriber, rows(0, 1));
        assertThat(subscriber.errorCount, equalTo(1));
        assertThat(subscriber.error, sameInstance(cause));
        assertFalse("onComplete must not fire after failStream", subscriber.completed);
    }

    public void testSynchronousRequestFromInsideOnNext() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.onFirstPage = subscriber::requestOne;
        subscriber.requestOne();

        producer.addPage(makePageWithValues(0, 1));
        producer.addPage(makePageWithValues(1, 1));
        producer.addPage(makePageWithValues(2, 1));
        assertThat("the demand requested from inside onNext must deliver the second page", subscriber.receivedPages, hasSize(2));

        subscriber.requestOne();
        finishStream(producer, publisher);
        expectPages(subscriber, rows(0, 1), rows(1, 1), rows(2, 1));
    }

    public void testReentrantAddPageFromUnblockListener() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribe(publisher);
        Page page = makePageWithValues(0, 1);
        publisher.waitForWriting().listener().addListener(ActionListener.running(() -> producer.addPage(page)));

        subscriber.requestOne();
        assertThat(subscriber.receivedPages, hasSize(1));
        assertThat(subscriber.error, nullValue());
        releasePages(subscriber.receivedPages);
    }

    public void testProducerKeepsAddingAfterCancellation() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.requestOne();
        subscriber.onFirstPage = subscriber::cancel;

        assertTrue(producer.addPage(makePageWithValues(0, 3)));
        assertFalse("a page added after the subscriber cancelled must be rejected", producer.addPage(makePageWithValues(3, 3)));
        finishStream(producer, publisher);

        releasePages(subscriber.receivedPages);
    }

    public void testConcurrentProducerAndDisruption() {
        final int numProducers = randomIntBetween(1, 4);
        final int totalPages = randomIntBetween(5, 30);
        final int[] rowsPerPage = new int[totalPages];
        for (int i = 0; i < totalPages; i++) {
            rowsPerPage[i] = randomIntBetween(1, 11);
        }
        final int disruptionType = randomIntBetween(0, 2);

        final BlockFactory factory = blockFactory();
        final PageStreamPublisher publisher = new PageStreamPublisher();

        List<PageStreamPublisher.Producer> producers = new ArrayList<>();
        for (int i = 0; i < numProducers; i++) {
            producers.add(publisher.registerProducer());
        }

        TestSubscriber subscriber = subscribe(publisher);
        subscriber.releaseOnNext = true;
        subscriber.requestN(Long.MAX_VALUE);

        // Each producer handles a round-robin slice of the pages.
        List<Runnable> producerTasks = new ArrayList<>();
        for (int p = 0; p < numProducers; p++) {
            final int producerIndex = p;
            final PageStreamPublisher.Producer producer = producers.get(p);
            producerTasks.add(() -> {
                for (int i = producerIndex; i < totalPages; i += numProducers) {
                    producer.addPage(makePageWithValues(factory, (long) i * 1000, rowsPerPage[i]));
                }
                producer.finish();
                // The highest-indexed producer also calls completeWithFooter; scheduling order
                // is non-deterministic, but SEND_COMPLETE requires pagesFinished regardless.
                if (producerIndex == numProducers - 1) {
                    publisher.completeWithFooter(0, List.of(), false);
                }
            });
        }

        final Runnable disruptorTask = () -> {
            switch (disruptionType) {
                case 1 -> subscriber.cancel();
                case 2 -> publisher.failStream(new RuntimeException("stress test disruption"));
                default -> {
                }
            }
        };

        Runnable[] allTasks = new Runnable[numProducers + 1];
        producerTasks.toArray(allTasks);
        allTasks[numProducers] = disruptorTask;
        startInParallel(allTasks);

        int terminalCount = subscriber.errorCount + (subscriber.completed ? 1 : 0);
        assertTrue("at most one terminal signal expected", terminalCount <= 1);
        if (disruptionType == 0) {
            assertEquals("onComplete expected with no disruption", 1, terminalCount);
        }
    }

    /**
     * Two producers share one publisher. After producer A consumes the outstanding demand,
     * producer B's wait-for-writing gate must not be left dangling: it must complete once
     * new demand is granted, and no {@link AssertionError} must escape.
     */
    public void testTwoProducersShareThePublisher() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer1 = publisher.registerProducer();
        PageStreamPublisher.Producer producer2 = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        producer1.addPage(makePageWithValues(0, 1));
        expectPage(subscriber, rows(0, 1));

        assertDriverBlocked(publisher);

        subscriber.requestOne();
        assertDriverUnblocked(publisher);

        producer2.addPage(makePageWithValues(1, 1));
        expectPage(subscriber, rows(1, 1));

        subscriber.requestOne();
        producer1.finish();
        producer2.finish();
        publisher.completeWithFooter(0, List.of(), false);
        assertTrue("stream must complete", subscriber.completed);
    }

    /**
     * When producer A finishes while producer B is still active, the stream must not complete: completion
     * needs every producer to call {@link PageStreamPublisher.Producer#finish()}.
     */
    public void testCompletionWaitsForAllProducers() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer1 = publisher.registerProducer();
        PageStreamPublisher.Producer producer2 = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        producer1.addPage(makePageWithValues(0, 2));
        expectPage(subscriber, rows(0, 2));
        producer1.finish();
        publisher.completeWithFooter(0, List.of(), false);

        subscriber.requestOne();
        assertFalse("onComplete must wait for the other producer", subscriber.completed);

        producer2.addPage(makePageWithValues(2, 8));
        expectPage(subscriber, rows(2, 8));
        subscriber.requestOne();
        assertFalse("onComplete must wait until the last producer finishes", subscriber.completed);

        producer2.finish();
        assertTrue("stream must complete once the last producer finishes", subscriber.completed);
    }

    private TestSubscriber subscribe(PageStreamPublisher publisher) {
        TestSubscriber subscriber = new TestSubscriber(publisher);
        allSubscribers.add(subscriber);
        publisher.subscribe(subscriber);
        return subscriber;
    }

    private TestSubscriber subscribeWithDemand(PageStreamPublisher publisher) {
        TestSubscriber subscriber = subscribe(publisher);
        subscriber.requestOne();
        return subscriber;
    }

    private static void finishStream(PageStreamPublisher.Producer producer, PageStreamPublisher publisher) {
        producer.finish();
        publisher.completeWithFooter(0, List.of(), false);
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
        IsBlockedResult blocked = publisher.waitForWriting();
        assertFalse(blocked.listener().isDone());
        assertThat(blocked.reason(), equalTo("streaming_page_consumer"));
    }

    private static void assertDriverUnblocked(PageStreamPublisher publisher) {
        assertTrue(publisher.waitForWriting().listener().isDone());
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

    private static class TestSubscriber implements Flow.Subscriber<Page> {
        private final PageStreamPublisher publisher;
        private Flow.Subscription subscription;
        final List<Page> receivedPages = new ArrayList<>();
        final List<String> lockViolations = new ArrayList<>();
        Throwable error;
        int errorCount;
        boolean completed;
        volatile boolean releaseOnNext;
        Runnable onFirstPage;

        TestSubscriber(PageStreamPublisher publisher) {
            this.publisher = publisher;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            if (Thread.holdsLock(publisher)) lockViolations.add("onSubscribe");
            this.subscription = s;
        }

        @Override
        public void onNext(Page page) {
            if (Thread.holdsLock(publisher)) lockViolations.add("onNext");
            if (releaseOnNext) {
                page.releaseBlocks();
            } else {
                receivedPages.add(page);
            }
            Runnable hook = onFirstPage;
            if (hook != null) {
                onFirstPage = null;
                hook.run();
            }
        }

        @Override
        public void onError(Throwable t) {
            if (Thread.holdsLock(publisher)) lockViolations.add("onError");
            this.error = t;
            this.errorCount++;
        }

        @Override
        public void onComplete() {
            if (Thread.holdsLock(publisher)) lockViolations.add("onComplete");
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

    public void testRowsPublishedCountsAcrossProducers() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer1 = publisher.registerProducer();
        PageStreamPublisher.Producer producer2 = publisher.registerProducer();
        TestSubscriber subscriber = subscribeWithDemand(publisher);

        assertThat("initial rowsPublished must be 0", publisher.rowsPublished(), equalTo(0L));

        producer1.addPage(makePageWithValues(0, 3));
        assertThat(publisher.rowsPublished(), equalTo(3L));

        producer2.addPage(makePageWithValues(3, 7));
        assertThat(publisher.rowsPublished(), equalTo(10L));

        producer1.addPage(makePageWithValues(10, 5));
        assertThat(publisher.rowsPublished(), equalTo(15L));

        producer1.finish();
        producer2.finish();

        subscriber.requestN(2);
        expectPages(subscriber, rows(0, 3), rows(3, 7), rows(10, 5));

        subscriber.requestOne();
        publisher.completeWithFooter(0, List.of(), false);
        assertTrue("stream must complete", subscriber.completed);
        assertThat(publisher.rowsPublished(), equalTo(15L));
    }

    public void testRowsPublishedDoesNotCountRejectedPages() {
        PageStreamPublisher publisher = new PageStreamPublisher();
        PageStreamPublisher.Producer producer = publisher.registerProducer();
        subscribe(publisher);

        assertTrue(producer.addPage(makePageWithValues(0, 4)));
        assertThat(publisher.rowsPublished(), equalTo(4L));

        publisher.failStream(new RuntimeException("terminal"));
        assertFalse("addPage must return false after terminal state", producer.addPage(makePageWithValues(4, 6)));
        assertThat("rejected page rows must not be counted", publisher.rowsPublished(), equalTo(4L));
    }
}
