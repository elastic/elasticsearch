/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * The result collector and the pages it hands over: a result under the budget stays a list; one over it goes to a
 * run, in order, read back through cursors that may be opened again, with the per-column facts captured on the way;
 * the sink's blocked state follows the run's creation and the appends in flight; a close releases everything.
 */
public class ResultCollectorTests extends ComputeTestCase {

    private TestThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() throws Exception {
        closeSpillingScopes();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testUnderTheBudgetTheResultStaysInMemory() throws Exception {
        MemorySpillStore store = inlineStore();
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofMb(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            AtomicBoolean released = new AtomicBoolean();
            ResultCollector collector = new ResultCollector(
                ctx.queryScope(),
                () -> released.set(true),
                ByteSizeValue.ofMb(1).getBytes(),
                "r"
            );
            List<List<Object>> expected = feed(ctx.blockFactory(), collector, between(1, 20), between(1, 100));
            assertFalse(collector.spilled());
            try (ResultPages pages = finish(collector)) {
                assertFalse(pages.spilled());
                assertThat(pages.rows(), equalTo((long) expected.size()));
                assertThat(rows(pages), equalTo(expected));
                assertThat(rows(pages), equalTo(expected));
                assertThat("the lease goes with a resident result at the finish", released.get(), equalTo(true));
            }
            assertThat(store.operationsAccepted(), equalTo(0));
        } finally {
            closeSpillingScopes();
            shutdown(store);
        }
    }

    public void testOverTheBudgetTheResultGoesToARunInOrder() throws Exception {
        MemorySpillStore store = inlineStore();
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofMb(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            AtomicBoolean released = new AtomicBoolean();
            ResultCollector collector = new ResultCollector(ctx.queryScope(), () -> released.set(true), between(0, 2_000), "r");
            List<List<Object>> expected = feed(ctx.blockFactory(), collector, between(5, 50), between(1, 200));
            assertTrue("the budget was crossed", collector.spilled());
            assertThat(collector.rowsSpilled(), equalTo((long) expected.size()));
            ResultPages pages = finish(collector);
            try {
                assertThat(pages, instanceOf(ResultPages.Spilled.class));
                assertThat(pages.rows(), equalTo((long) expected.size()));
                assertThat("the pages read back in order", rows(pages), equalTo(expected));
                assertThat("a second cursor reads them again", rows(pages), equalTo(expected));
                assertFalse("the lease stays with the result", released.get());
                expectThrows(IllegalStateException.class, pages::resident);
            } finally {
                pages.close();
            }
            assertTrue("the lease goes with the result", released.get());
            assertThat("the run was written and read", store.operationsAccepted(), greaterThan(0));
        } finally {
            closeSpillingScopes();
            shutdown(store);
        }
    }

    /**
     * The per-column facts: a column null throughout and one with multivalues are known to the result without a walk.
     */
    public void testColumnFactsAreCapturedAsThePagesCome() throws Exception {
        MemorySpillStore store = inlineStore();
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofMb(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            boolean spill = randomBoolean();
            ResultCollector collector = new ResultCollector(ctx.queryScope(), null, spill ? 0 : Long.MAX_VALUE, "r");
            BlockFactory factory = ctx.blockFactory();
            int pages = between(2, 10);
            for (int p = 0; p < pages; p++) {
                int rows = between(1, 20);
                try (
                    LongBlock.Builder keys = factory.newLongBlockBuilder(rows);
                    BytesRefBlock.Builder nulls = factory.newBytesRefBlockBuilder(rows);
                    LongBlock.Builder multi = factory.newLongBlockBuilder(rows)
                ) {
                    for (int r = 0; r < rows; r++) {
                        keys.appendLong(r);
                        nulls.appendNull();
                        if (p == pages - 1 && r == 0) {
                            multi.beginPositionEntry().appendLong(1).appendLong(2).endPositionEntry();
                        } else {
                            multi.appendLong(r);
                        }
                    }
                    collector.accept(new Page(keys.build(), nulls.build(), multi.build()));
                }
            }
            assertThat(collector.spilled(), equalTo(spill));
            try (ResultPages result = finish(collector)) {
                assertThat(result.pageCount(), equalTo(pages));
                assertArrayEquals(new boolean[] { false, true, false }, result.nullColumns());
                assertArrayEquals(new boolean[] { false, false, true }, result.multivaluedColumns());
            }
        } finally {
            closeSpillingScopes();
            shutdown(store);
        }
    }

    /**
     * With a store that holds every completion, the sink is blocked while the run is created and while too many
     * appends are queued, and the pages still read back in order once everything is released.
     */
    public void testADelayedStoreBlocksTheSink() throws Exception {
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            threadPool.generic(),
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofMb(1), 0, store, threadPool.executor(ThreadPool.Names.SEARCH));
            ResultCollector collector = new ResultCollector(ctx.queryScope(), null, 0, "r");
            BlockFactory factory = ctx.blockFactory();
            List<List<Object>> expected = new ArrayList<>();
            int pages = between(4, 20);
            int rounds = 0;
            for (int p = 0; p < pages; p++) {
                while (collector.isBlocked().listener().isDone() == false) {
                    // the run's creation and the chunk pushes are held by the store; an append completes on the codec pool
                    assertThat("the collector makes progress", ++rounds, lessThanOrEqualTo(1_000_000));
                    if (store.heldOperations() > 0) {
                        assertTrue("blocked on a held completion", store.releaseRandom());
                    } else {
                        Thread.sleep(1);
                    }
                }
                collector.accept(page(factory, p, between(1, 50), expected));
                if (p == 0) {
                    assertTrue("the first page crossed the budget", collector.spilled());
                    assertFalse("the run is being created", collector.isBlocked().listener().isDone());
                }
            }
            PlainActionFuture<ResultPages> finished = new PlainActionFuture<>();
            collector.finish(finished);
            releaseUntilDone(store, finished);
            try (ResultPages result = finished.get()) {
                assertTrue(result.spilled());
                List<List<Object>> read = new ArrayList<>();
                try (ResultPages.Cursor cursor = result.open()) {
                    while (cursor.hasNext()) {
                        PlainActionFuture<Page> next = new PlainActionFuture<>();
                        cursor.next(next);
                        releaseUntilDone(store, next);
                        Page page = next.get();
                        try {
                            collect(page, read);
                        } finally {
                            page.releaseBlocks();
                        }
                    }
                }
                assertThat(read, equalTo(expected));
            }
        } finally {
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            releaseUntilDone(store, shutdown);
        }
    }

    /**
     * Closed while the store still holds the run's creation: the pages in memory go at once and the lease with them, and
     * the writer the creation brings is aborted, so the store ends with nothing of the run.
     */
    public void testCloseWhileTheRunIsBeingCreated() throws Exception {
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            threadPool.generic(),
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofMb(1), 0, store, threadPool.executor(ThreadPool.Names.SEARCH));
            AtomicBoolean released = new AtomicBoolean();
            ResultCollector collector = new ResultCollector(ctx.queryScope(), () -> released.set(true), 0, "r");
            List<List<Object>> expected = new ArrayList<>();
            int pages = between(1, 3);
            for (int p = 0; p < pages; p++) {
                // the pages wait in memory while the store holds the run's creation
                collector.accept(page(ctx.blockFactory(), p, between(1, 50), expected));
            }
            assertTrue(collector.spilled());
            assertFalse("the run is being created", collector.isBlocked().listener().isDone());
            collector.close();
            assertFalse("the lease waits for the run's teardown, which the store still holds", released.get());
            closeSpillingScopes();
            assertTrue("the lease went after the aborted run", released.get());
            assertThat("the store holds nothing of the run", store.bytesInUse(), equalTo(0L));
        } finally {
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            releaseUntilDone(store, shutdown);
        }
    }

    /**
     * Closed instead of finished, as a failed query closes it: the pages in memory go, the run is aborted and the lease
     * released, whether the budget was crossed or not.
     */
    public void testCloseReleasesEverything() throws Exception {
        MemorySpillStore store = inlineStore();
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofMb(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            AtomicBoolean released = new AtomicBoolean();
            boolean spill = randomBoolean();
            ResultCollector collector = new ResultCollector(ctx.queryScope(), () -> released.set(true), spill ? 0 : Long.MAX_VALUE, "r");
            feed(ctx.blockFactory(), collector, between(1, 10), between(1, 50));
            assertThat(collector.spilled(), equalTo(spill));
            collector.close();
            collector.close();
            assertTrue(released.get());
            PlainActionFuture<ResultPages> late = new PlainActionFuture<>();
            collector.finish(late);
            expectThrows(IllegalStateException.class, late::actionGet);
        } finally {
            closeSpillingScopes();
            shutdown(store);
        }
    }

    /**
     * A store that refuses the run's bytes: the failure is thrown at the page that follows it or reported at the finish,
     * and the close afterwards releases everything the collector holds.
     */
    public void testAFailedRunIsReportedAndReleased() throws Exception {
        MemorySpillStore store = new MemorySpillStore(MemorySpillStore.Completion.INLINE, null, 1, "test.spill", new Random(randomLong()));
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofMb(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            AtomicBoolean released = new AtomicBoolean();
            ResultCollector collector = new ResultCollector(ctx.queryScope(), () -> released.set(true), 0, "r");
            BlockFactory factory = ctx.blockFactory();
            List<List<Object>> expected = new ArrayList<>();
            Exception failure = null;
            for (int p = 0; p < 40 && failure == null; p++) {
                try {
                    collector.accept(page(factory, p, 1_000, expected));
                } catch (Exception e) {
                    failure = e;
                }
            }
            if (failure == null) {
                PlainActionFuture<ResultPages> finished = new PlainActionFuture<>();
                collector.finish(finished);
                failure = expectThrows(Exception.class, finished::actionGet);
            }
            assertThat("the store's limit is the cause", ExceptionsHelper.stackTrace(failure), containsString("test.spill"));
            collector.close();
            assertTrue(released.get());
        } finally {
            closeSpillingScopes();
            shutdown(store);
        }
    }

    /**
     * Closed after the finish, as the query's failure path may do once the result is on its way: the result is left
     * alone, the run read back whole, whether the seal had landed or was still held by the store.
     */
    public void testCloseAfterTheFinishLeavesTheResultAlone() throws Exception {
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            threadPool.generic(),
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofMb(1), 0, store, threadPool.executor(ThreadPool.Names.SEARCH));
            AtomicBoolean released = new AtomicBoolean();
            ResultCollector collector = new ResultCollector(ctx.queryScope(), () -> released.set(true), 0, "r");
            BlockFactory factory = ctx.blockFactory();
            List<List<Object>> expected = new ArrayList<>();
            int pages = between(1, 6);
            for (int p = 0; p < pages; p++) {
                while (collector.isBlocked().listener().isDone() == false) {
                    if (store.heldOperations() > 0) {
                        store.releaseRandom();
                    } else {
                        Thread.sleep(1);
                    }
                }
                collector.accept(page(factory, p, between(1, 50), expected));
            }
            PlainActionFuture<ResultPages> finished = new PlainActionFuture<>();
            collector.finish(finished);
            if (randomBoolean()) {
                releaseUntilDone(store, finished);
            }
            collector.close();
            assertFalse("the lease is the result's now", released.get());
            releaseUntilDone(store, finished);
            try (ResultPages result = finished.get()) {
                assertTrue(result.spilled());
                List<List<Object>> read = new ArrayList<>();
                try (ResultPages.Cursor cursor = result.open()) {
                    while (cursor.hasNext()) {
                        PlainActionFuture<Page> next = new PlainActionFuture<>();
                        cursor.next(next);
                        releaseUntilDone(store, next);
                        Page page = next.get();
                        try {
                            collect(page, read);
                        } finally {
                            page.releaseBlocks();
                        }
                    }
                }
                assertThat(read, equalTo(expected));
            }
            closeSpillingScopes();
            assertTrue("the lease went with the result", released.get());
        } finally {
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            releaseUntilDone(store, shutdown);
        }
    }

    /**
     * The budget is read at every page: a pragma applied after the collector exists takes effect at the next page.
     */
    public void testTheBudgetIsReadAtEveryPage() throws Exception {
        MemorySpillStore store = inlineStore();
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofMb(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            AtomicLong budget = new AtomicLong(Long.MAX_VALUE);
            ResultCollector collector = new ResultCollector(ctx.queryScope(), null, budget::get, "r");
            List<List<Object>> expected = new ArrayList<>();
            collector.accept(page(ctx.blockFactory(), 0, between(1, 50), expected));
            assertFalse("under a budget without bound", collector.spilled());
            budget.set(0);
            collector.accept(page(ctx.blockFactory(), 1, between(1, 50), expected));
            assertTrue("over the budget the next page saw", collector.spilled());
            try (ResultPages result = finish(collector)) {
                assertThat(rows(result), equalTo(expected));
            }
        } finally {
            closeSpillingScopes();
            shutdown(store);
        }
    }

    /**
     * Without a scope the collector is a list, whatever the budget.
     */
    public void testWithoutAScopeTheCollectorIsAList() throws Exception {
        BlockFactory factory = blockFactory();
        ResultCollector collector = new ResultCollector(null, null, 0, "r");
        List<List<Object>> expected = feed(factory, collector, between(1, 10), between(1, 50));
        assertFalse(collector.spilled());
        assertTrue(collector.isBlocked().listener().isDone());
        try (ResultPages pages = finish(collector)) {
            assertFalse(pages.spilled());
            assertThat(rows(pages), equalTo(expected));
        }
    }

    // ---- helpers

    private static List<List<Object>> feed(BlockFactory factory, ResultCollector collector, int pages, int maxRows) {
        List<List<Object>> expected = new ArrayList<>();
        for (int p = 0; p < pages; p++) {
            assertTrue("an inline store never blocks the sink", collector.isBlocked().listener().isDone());
            collector.accept(page(factory, p, between(1, maxRows), expected));
        }
        return expected;
    }

    private static Page page(BlockFactory factory, int pageNumber, int rows, List<List<Object>> expected) {
        try (
            LongBlock.Builder keys = factory.newLongBlockBuilder(rows);
            BytesRefBlock.Builder values = factory.newBytesRefBlockBuilder(rows)
        ) {
            for (int r = 0; r < rows; r++) {
                long key = pageNumber * 1_000L + r;
                String value = randomAlphaOfLengthBetween(1, 200);
                keys.appendLong(key);
                values.appendBytesRef(new BytesRef(value));
                expected.add(List.of(key, value));
            }
            return new Page(keys.build(), values.build());
        }
    }

    private static ResultPages finish(ResultCollector collector) {
        PlainActionFuture<ResultPages> future = new PlainActionFuture<>();
        collector.finish(future);
        return future.actionGet();
    }

    /**
     * The rows through a cursor of the result, read blocking; the pages a run hands out are released here.
     */
    private static List<List<Object>> rows(ResultPages pages) {
        List<List<Object>> rows = new ArrayList<>();
        try (ResultPages.Cursor cursor = pages.open()) {
            while (cursor.hasNext()) {
                Page page = cursor.nextBlocking();
                try {
                    collect(page, rows);
                } finally {
                    page.releaseBlocks();
                }
            }
            assertThat(cursor.nextBlocking(), nullValue());
        }
        return rows;
    }

    private static void collect(Page page, List<List<Object>> into) {
        LongBlock keys = page.getBlock(0);
        BytesRefBlock values = page.getBlock(1);
        BytesRef scratch = new BytesRef();
        for (int p = 0; p < page.getPositionCount(); p++) {
            into.add(List.of(keys.getLong(p), values.getBytesRef(p, scratch).utf8ToString()));
        }
    }

    private static MemorySpillStore inlineStore() {
        return new MemorySpillStore(MemorySpillStore.Completion.INLINE, null, Long.MAX_VALUE, "test.spill", new Random(randomLong()));
    }

    private static void shutdown(MemorySpillStore store) throws Exception {
        PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
        store.shutdown(shutdown);
        shutdown.get(30, TimeUnit.SECONDS);
    }

    private static void releaseUntilDone(MemorySpillStore store, PlainActionFuture<?> done) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (done.isDone() == false) {
            if (store.releaseAll() > 0) {
                continue;
            }
            assertTrue("the store's operations did not complete in time", System.nanoTime() < deadline);
            Thread.sleep(1);
        }
    }
}
