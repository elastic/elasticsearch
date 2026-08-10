/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Contract tests for {@link BufferingPageIterator} — the one place where the external-format page
 * iterators (NDJSON / CSV / parquet-rs) release the single buffered look-ahead {@link Page} on
 * {@code close()}. The reader-level regressions live next to each reader; here we pin the base
 * class's invariants directly with a fake subclass so a future refactor of any one reader can't
 * silently reintroduce the leak the base class was created to kill.
 */
public class BufferingPageIteratorTests extends ESTestCase {

    private BigArrays bigArrays;
    private CircuitBreaker breaker;
    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(64)).withCircuitBreaking();
        breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        blockFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();
    }

    /** A page whose single block is allocated against the tracking breaker, so a leak shows as non-zero used bytes. */
    private Page trackedPage(int positions) {
        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendInt(i);
            }
            return new Page(builder.build());
        }
    }

    /**
     * Minimal {@link BufferingPageIterator}: hands out pages from a queue using the inherited
     * {@code nextPage} buffer exactly the way the production readers do, and records its teardown.
     */
    private static final class FakeBufferingIterator extends BufferingPageIterator {
        private final Deque<Page> source;
        final AtomicInteger closeInternalCalls = new AtomicInteger();
        volatile boolean nextPageWasNullInCloseInternal;
        IOException closeInternalFailure;

        FakeBufferingIterator(List<Page> pages) {
            this.source = new ArrayDeque<>(pages);
        }

        @Override
        public boolean hasNext() {
            if (nextPage != null) {
                return true;
            }
            if (source.isEmpty()) {
                return false;
            }
            nextPage = source.poll();
            return true;
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            Page p = nextPage;
            nextPage = null;
            return p;
        }

        @Override
        protected void closeInternal() throws IOException {
            // The base must have captured and nulled the buffered page before delegating here.
            nextPageWasNullInCloseInternal = nextPage == null;
            closeInternalCalls.incrementAndGet();
            if (closeInternalFailure != null) {
                throw closeInternalFailure;
            }
        }
    }

    /** Releases any pages still queued (never handed out) so a test's own un-iterated fixtures don't trip leak detection. */
    private static void drain(FakeBufferingIterator it) {
        while (it.source.isEmpty() == false) {
            it.source.poll().releaseBlocks();
        }
    }

    /** The core regression: a page materialized by {@code hasNext()} but never consumed is released on close. */
    public void testCloseReleasesBufferedPageMaterializedByHasNext() throws IOException {
        FakeBufferingIterator it = new FakeBufferingIterator(List.of(trackedPage(128)));
        assertTrue(it.hasNext());
        assertThat("hasNext must have materialized (and allocated) the look-ahead page", breaker.getUsed(), greaterThan(0L));

        it.close();

        assertEquals("the buffered page's blocks must be released on close", 0L, breaker.getUsed());
        assertEquals(1, it.closeInternalCalls.get());
        assertTrue("base must null the buffer before calling closeInternal", it.nextPageWasNullInCloseInternal);
    }

    /** Mid-stream early close: some pages consumed, one buffered, the rest never materialized — none may leak. */
    public void testCloseMidStreamReleasesBufferedAndNeverLeaks() throws IOException {
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pages.add(trackedPage(64));
        }
        FakeBufferingIterator it = new FakeBufferingIterator(pages);

        // Consume two pages fully (caller owns + releases those), then materialize a third and abandon it.
        it.next().releaseBlocks();
        it.next().releaseBlocks();
        assertTrue(it.hasNext()); // buffers page #3
        long usedWithBuffered = breaker.getUsed();
        assertThat(usedWithBuffered, greaterThan(0L));

        it.close(); // releases the buffered page #3
        drain(it); // pages #4, #5 were never handed out by the iterator; the test fixture owns them

        assertEquals(0L, breaker.getUsed());
        assertEquals(1, it.closeInternalCalls.get());
    }

    /** Closing before any {@code hasNext()} must still run teardown and must not NPE on the absent buffer. */
    public void testCloseWithNoBufferedPageStillRunsTeardown() throws IOException {
        FakeBufferingIterator it = new FakeBufferingIterator(List.of());
        it.close();
        assertEquals(0L, breaker.getUsed());
        assertEquals(1, it.closeInternalCalls.get());
        assertTrue(it.nextPageWasNullInCloseInternal);
    }

    /** After natural exhaustion the buffer is already null; close must not double-release and must not leak. */
    public void testCloseAfterFullConsumptionDoesNotDoubleRelease() throws IOException {
        List<Page> pages = List.of(trackedPage(32), trackedPage(32));
        FakeBufferingIterator it = new FakeBufferingIterator(pages);
        while (it.hasNext()) {
            it.next().releaseBlocks();
        }
        assertEquals("all consumed pages released by the caller", 0L, breaker.getUsed());

        it.close();

        assertEquals(0L, breaker.getUsed());
        assertEquals(1, it.closeInternalCalls.get());
    }

    /** {@code close()} is idempotent: a second call is a no-op — teardown runs once, no double-release of the page. */
    public void testCloseIsIdempotent() throws IOException {
        FakeBufferingIterator it = new FakeBufferingIterator(List.of(trackedPage(48)));
        assertTrue(it.hasNext());

        it.close();
        it.close();
        it.close();

        assertEquals(0L, breaker.getUsed());
        assertEquals("closeInternal must run exactly once across repeated close() calls", 1, it.closeInternalCalls.get());
    }

    /** A failure in subclass teardown still propagates, but the buffered page is released first (finally semantics). */
    public void testCloseInternalFailurePropagatesButPageStillReleased() {
        FakeBufferingIterator it = new FakeBufferingIterator(List.of(trackedPage(96)));
        it.closeInternalFailure = new IOException("teardown boom");
        assertTrue(it.hasNext());

        IOException thrown = expectThrows(IOException.class, it::close);
        assertEquals("teardown boom", thrown.getMessage());
        assertEquals("the buffered page must be released even when closeInternal throws", 0L, breaker.getUsed());
        assertEquals(1, it.closeInternalCalls.get());
    }

    /**
     * Concurrent close from many threads (a consumer thread racing a cancel/abort thread) must release the
     * buffered page exactly once and run teardown exactly once — the {@code releaseBlocks()} on an
     * already-released page would throw if the CAS let two callers in.
     */
    public void testConcurrentCloseReleasesExactlyOnce() throws Exception {
        FakeBufferingIterator it = new FakeBufferingIterator(List.of(trackedPage(256)));
        assertTrue(it.hasNext());

        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<Throwable> failures = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await();
                    it.close();
                } catch (Throwable e) {
                    failures.add(e);
                } finally {
                    done.countDown();
                }
            });
            t.start();
        }
        start.countDown();
        assertTrue("all closers finished", done.await(30, TimeUnit.SECONDS));

        assertTrue("close() must be safe under concurrency, saw: " + failures, failures.isEmpty());
        assertEquals(0L, breaker.getUsed());
        assertEquals("exactly one closer may win the CAS and run teardown", 1, it.closeInternalCalls.get());
    }
}
