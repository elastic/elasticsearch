/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.compute.operator.topn.TopNEncoder.DEFAULT_UNSORTABLE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * The value run of {@link TopNOperator}: under a budget every page crosses, the values of the rows kept go to segments
 * as the rows are kept, the rows evicted leave dead bytes that a compaction reclaims, and the output is the resident
 * run's, page for page and row for row, the values read back in physical order and returned in the sort's.
 */
public class TopNOperatorSpillTests extends ComputeTestCase {

    private TestThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() throws Exception {
        // the scopes' spilled data goes on the pool, before the pool stops
        closeSpillingScopes();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * An eviction-heavy stream: many more rows than are kept, so the dead bytes outgrow the live ones and the lane
     * compacts, more than once.
     */
    public void testEvictionsCompactAndTheOrderIsExact() throws Exception {
        int rows = between(20_000, 40_000);
        long[] keys = uniqueKeys(rows);
        BytesRef[] values = randomValues(rows, 100, 500);
        int topCount = between(500, 1_500);
        TopNOperatorStatus status = runAndCompare(keys, values, topCount, randomBoolean(), between(100, 1_000), null);
        assertThat(status.rowsSpilled(), greaterThan((long) topCount));
        assertThat("the dead bytes were reclaimed", status.compactions(), greaterThan(0));
        assertThat(status.spill().spillRuns(), greaterThan(status.compactions()));
        assertThat(status.spill().readBackBytes(), greaterThan(0L));
    }

    /**
     * Keys from a tiny domain: the rows tie on the key and the resident run's tie order is the spilling run's.
     */
    public void testTiesKeepTheirOrder() throws Exception {
        int rows = between(1_000, 5_000);
        long[] keys = new long[rows];
        for (int i = 0; i < rows; i++) {
            keys[i] = between(0, 3);
        }
        BytesRef[] values = randomValues(rows, 10, 100);
        TopNOperatorStatus status = runAndCompare(keys, values, between(10, 200), randomBoolean(), between(10, 500), null);
        assertThat(status.rowsSpilled(), greaterThan(0L));
    }

    /**
     * Rows inserted from the worst key to the best, so that each one enters and the physical order of the values is
     * the reverse of the output's: the gather reads in the one and returns in the other.
     */
    public void testPhysicalOrderIsNotTheOutputOrder() throws Exception {
        int rows = between(2_000, 5_000);
        long[] keys = new long[rows];
        for (int i = 0; i < rows; i++) {
            keys[i] = rows - i;
        }
        BytesRef[] values = randomValues(rows, 10, 100);
        TopNOperatorStatus status = runAndCompare(keys, values, between(100, 1_000), true, between(10, 500), null);
        assertThat("every row entered the heap", status.rowsSpilled(), equalTo((long) rows));
    }

    /**
     * Values larger than a segment's chunk, which span chunks in the run.
     */
    public void testLargeValues() throws Exception {
        int rows = between(10, 30);
        long[] keys = uniqueKeys(rows);
        BytesRef[] values = randomValues(rows, TopNValueStore.CHUNK_BYTES + 1, 4 * TopNValueStore.CHUNK_BYTES);
        TopNOperatorStatus status = runAndCompare(keys, values, between(3, 5), randomBoolean(), between(1, 10), null);
        assertThat(status.rowsSpilled(), greaterThan(0L));
        assertThat(status.spill().readBackBytes(), greaterThan((long) TopNValueStore.CHUNK_BYTES));
    }

    /**
     * A store too small for the compaction's new segment next to the old ones: the compaction fails, the rows keep
     * their pointers and the output is still exact.
     */
    public void testACompactionThatDoesNotFitFailsCleanly() throws Exception {
        int topCount = 1_000;
        int valueLength = 300;
        int rows = 2_300;
        long[] keys = new long[rows];
        for (int i = 0; i < rows; i++) {
            keys[i] = rows - i;
        }
        BytesRef[] values = new BytesRef[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = new BytesRef(randomByteArrayOfLength(valueLength));
        }
        // the live values are 300 KB, the dead ones outgrow them past two thousand rows, and the compaction's copy of
        // the live values would take the store past its limit
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.INLINE,
            null,
            ByteSizeValue.ofKb(800).getBytes(),
            "test.spill",
            new Random(randomLong())
        );
        List<Page> expected = runResident(keys, values, topCount, true, 100, null);
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            TopNOperator op = factory(topCount, true, 100, null).get(ctx);
            List<Page> actual = new TestDriverRunner().builder(ctx).input(input(ctx.blockFactory(), keys, values, 100)).run(op);
            try {
                assertRows(expected, actual);
                assertThat(op.valueStore(), not(nullValue()));
                assertThat("the compaction was refused", op.valueStore().compactionsFailed(), equalTo(1));
                assertThat(op.valueStore().compactions(), equalTo(0));
            } finally {
                Releasables.close(actual);
            }
        } finally {
            Releasables.close(expected);
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            shutdown.get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * The parallel variant: every worker spills into its lane of the shared store, and the merge gathers across them.
     */
    public void testParallelWorkersSpill() throws Exception {
        int rows = between(10_000, 30_000);
        long[] keys = uniqueKeys(rows);
        BytesRef[] values = randomValues(rows, 10, 200);
        var parallel = new TopNOperator.ParallelWorkerConfig(threadPool.executor(ThreadPool.Names.SEARCH), between(1, 4), 4, 100);
        runAndCompare(keys, values, between(100, 1_000), randomBoolean(), between(100, 1_000), parallel);
    }

    /**
     * Closed with a gather in flight, as a cancelled driver closes it: the read lands into nothing and everything
     * borrowed goes back. The values are wider than a reader's window, so that every page's gather reads the store.
     */
    public void testCloseWithAGatherInFlightReleasesEverything() throws Exception {
        int rows = between(6, 12);
        long[] keys = uniqueKeys(rows);
        BytesRef[] values = randomValues(rows, 3 * TopNValueStore.CHUNK_BYTES, 4 * TopNValueStore.CHUNK_BYTES);
        int topCount = between(3, 5);
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            threadPool.generic(),
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        List<Page> output = new ArrayList<>();
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, threadPool.executor(ThreadPool.Names.SEARCH));
            TopNOperator op = factory(topCount, true, 1, null).get(ctx);
            List<Page> input = input(ctx.blockFactory(), keys, values, between(1, 3));
            int rounds = 0;
            for (Page page : input) {
                while (op.needsInput() == false) {
                    assertThat("the operator makes progress", ++rounds, lessThanOrEqualTo(100_000));
                    if (op.isBlocked().listener().isDone() == false) {
                        awaitHeld(store);
                        assertTrue("blocked on a held completion", store.releaseRandom());
                    }
                }
                op.addInput(page);
            }
            op.finish();
            Page first = null;
            while (first == null) {
                assertThat("the operator makes progress", ++rounds, lessThanOrEqualTo(100_000));
                if (op.isBlocked().listener().isDone() == false) {
                    awaitHeld(store);
                    assertTrue("blocked on a held completion", store.releaseRandom());
                    continue;
                }
                first = op.getOutput();
            }
            output.add(first);
            assertThat(first.getPositionCount(), equalTo(1));
            // the next pages until one's gather is issued and its read held by the store; a row admitted resident while
            // the lane could not take it decodes without a read
            boolean gatherInFlight = false;
            while (op.isFinished() == false) {
                Page next = op.getOutput();
                if (next != null) {
                    output.add(next);
                    continue;
                }
                if (op.isBlocked().listener().isDone() == false) {
                    gatherInFlight = true;
                    break;
                }
            }
            assumeTrue("a gather is held by the store", gatherInFlight);
            op.close();
            ctx.finish();
            PlainActionFuture<Void> quiet = new PlainActionFuture<>();
            ctx.waitForAsyncActions(quiet);
            releaseUntilDone(store, quiet);
        } finally {
            Releasables.close(output);
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            releaseUntilDone(store, shutdown);
        }
    }

    /**
     * The parallel variant over a store that holds every completion: the workers' lanes take pages without the
     * operator's say and the merge target may be fed while its lane has no writer, so rows stay resident until the
     * lane is ready and the merged result mixes resident and spilled rows. The output is the resident run's.
     */
    public void testParallelWorkersWithADelayedStore() throws Exception {
        int rows = between(5_000, 10_000);
        long[] keys = uniqueKeys(rows);
        BytesRef[] values = randomValues(rows, 10, 100);
        int topCount = between(100, 500);
        int pageRows = between(10, 100);
        var parallel = new TopNOperator.ParallelWorkerConfig(threadPool.executor(ThreadPool.Names.SEARCH), between(1, 4), 4, 100);
        List<Page> expected = runResident(keys, values, topCount, true, pageRows, parallel);
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            threadPool.generic(),
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        List<Page> output = new ArrayList<>();
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, threadPool.executor(ThreadPool.Names.GENERIC));
            Operator op = factory(topCount, true, pageRows, parallel).get(ctx);
            try {
                List<Page> input = input(ctx.blockFactory(), keys, values, between(10, 100));
                int rounds = 0;
                for (Page page : input) {
                    while (op.needsInput() == false) {
                        assertThat("the operator makes progress", ++rounds, lessThanOrEqualTo(1_000_000));
                        nudge(store, op);
                    }
                    op.addInput(page);
                    op = op.tryPromote(ctx);
                }
                assertThat("the operator was promoted", op, instanceOf(ParallelTopNOperator.class));
                op.finish();
                while (op.isFinished() == false) {
                    assertThat("the operator makes progress", ++rounds, lessThanOrEqualTo(1_000_000));
                    if (op.isBlocked().listener().isDone() == false) {
                        nudge(store, op);
                        continue;
                    }
                    Page page = op.getOutput();
                    if (page != null) {
                        output.add(page);
                    }
                }
            } finally {
                op.close();
            }
            ctx.finish();
            PlainActionFuture<Void> quiet = new PlainActionFuture<>();
            ctx.waitForAsyncActions(quiet);
            releaseUntilDone(store, quiet);
            assertThat("the run spilled", store.operationsAccepted(), greaterThan(0));
            assertRows(expected, output);
        } finally {
            Releasables.close(output);
            Releasables.close(expected);
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            releaseUntilDone(store, shutdown);
        }
    }

    /**
     * Closed with appends in flight, as a cancelled driver closes it: the abort fails what the codec still holds, the
     * driver's wait for the appends ends and what they held goes back. The values fill a chunk each, so that the codec
     * parks on the store holding the chunks and the appends are still in flight at the close.
     */
    public void testCloseWithAppendsInFlightReleasesEverything() throws Exception {
        int rows = between(6, 12);
        long[] keys = uniqueKeys(rows);
        BytesRef[] values = randomValues(rows, TopNValueStore.CHUNK_BYTES, 2 * TopNValueStore.CHUNK_BYTES);
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            threadPool.generic(),
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, threadPool.executor(ThreadPool.Names.SEARCH));
            TopNOperator op = factory(between(3, 5), true, 1, null).get(ctx);
            List<Page> input = input(ctx.blockFactory(), keys, values, between(1, 3));
            int rounds = 0;
            for (Page page : input) {
                while (op.needsInput() == false) {
                    assertThat("the operator makes progress", ++rounds, lessThanOrEqualTo(100_000));
                    if (op.isBlocked().listener().isDone() == false) {
                        awaitHeld(store);
                        assertTrue("blocked on a held completion", store.releaseRandom());
                    }
                }
                op.addInput(page);
            }
            assertThat("values went to the lane", ((TopNOperatorStatus) op.status()).rowsSpilled(), greaterThan(0L));
            awaitHeld(store);
            op.close();
            ctx.finish();
            PlainActionFuture<Void> quiet = new PlainActionFuture<>();
            ctx.waitForAsyncActions(quiet);
            releaseUntilDone(store, quiet);
        } finally {
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            releaseUntilDone(store, shutdown);
        }
    }

    /**
     * With a store that holds every completion, the operator reports blocked rather than spinning, and the output is
     * still exact.
     */
    public void testADelayedStoreBlocksTheOperator() throws Exception {
        int rows = between(500, 3_000);
        long[] keys = uniqueKeys(rows);
        BytesRef[] values = randomValues(rows, 10, 100);
        int topCount = between(50, 300);
        int pageRows = between(5, 50);
        List<Page> expected = runResident(keys, values, topCount, true, pageRows, null);
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.DEFERRED,
            threadPool.generic(),
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        List<Page> output = new ArrayList<>();
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, threadPool.executor(ThreadPool.Names.SEARCH));
            try (TopNOperator op = factory(topCount, true, pageRows, null).get(ctx)) {
                List<Page> input = input(ctx.blockFactory(), keys, values, between(10, 100));
                int rounds = 0;
                for (Page page : input) {
                    while (op.needsInput() == false) {
                        assertThat("the operator makes progress", ++rounds, lessThanOrEqualTo(100_000));
                        if (op.isBlocked().listener().isDone() == false) {
                            awaitHeld(store);
                            assertTrue("blocked on a held completion", store.releaseRandom());
                        }
                    }
                    op.addInput(page);
                }
                op.finish();
                while (op.isFinished() == false) {
                    assertThat("the operator makes progress", ++rounds, lessThanOrEqualTo(100_000));
                    if (op.isBlocked().listener().isDone() == false) {
                        awaitHeld(store);
                        assertTrue("blocked on a held completion", store.releaseRandom());
                        continue;
                    }
                    Page page = op.getOutput();
                    if (page != null) {
                        output.add(page);
                    }
                }
                assertThat(((TopNOperatorStatus) op.status()).rowsSpilled(), greaterThan(0L));
            }
            ctx.finish();
            PlainActionFuture<Void> quiet = new PlainActionFuture<>();
            ctx.waitForAsyncActions(quiet);
            releaseUntilDone(store, quiet);
            assertRows(expected, output);
        } finally {
            Releasables.close(output);
            Releasables.close(expected);
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            releaseUntilDone(store, shutdown);
        }
    }

    // ---- the runs

    /**
     * Runs the operator over the rows resident and under a budget of one byte, with an in-memory store that completes
     * inline, and checks the two outputs match row for row; returns the spilling run's status.
     */
    private TopNOperatorStatus runAndCompare(
        long[] keys,
        BytesRef[] values,
        int topCount,
        boolean asc,
        int pageRows,
        @Nullable TopNOperator.ParallelWorkerConfig parallel
    ) throws Exception {
        List<Page> expected = runResident(keys, values, topCount, asc, pageRows, parallel);
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.INLINE,
            null,
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            TopNOperator op = factory(topCount, asc, pageRows, parallel).get(ctx);
            List<Page> actual = new TestDriverRunner().builder(ctx)
                .input(input(ctx.blockFactory(), keys, values, between(10, 1_000)))
                .run(op);
            try {
                assertThat("the run spilled", store.operationsAccepted(), greaterThan(0));
                assertRows(expected, actual);
                return (TopNOperatorStatus) op.status();
            } finally {
                Releasables.close(actual);
            }
        } finally {
            Releasables.close(expected);
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            shutdown.get(30, TimeUnit.SECONDS);
        }
    }

    private List<Page> runResident(
        long[] keys,
        BytesRef[] values,
        int topCount,
        boolean asc,
        int pageRows,
        @Nullable TopNOperator.ParallelWorkerConfig parallel
    ) {
        var blockFactory = blockFactory();
        DriverContext ctx = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        List<Page> input = input(blockFactory, keys, values, between(10, 1_000));
        List<Page> output = new TestDriverRunner().builder(ctx).input(input).run(factory(topCount, asc, pageRows, parallel).get(ctx));
        // the pages outlive the context's breaker checks: copy them out
        List<Page> copy = BlockTestUtils.deepCopyOf(output, TestBlockFactory.getNonBreakingInstance());
        Releasables.close(output);
        return copy;
    }

    private static TopNOperator.TopNOperatorFactory factory(
        int topCount,
        boolean asc,
        int pageRows,
        @Nullable TopNOperator.ParallelWorkerConfig parallel
    ) {
        return new TopNOperator.TopNOperatorFactory(
            topCount,
            List.of(ElementType.LONG, ElementType.BYTES_REF),
            List.of(DEFAULT_UNSORTABLE, DEFAULT_UNSORTABLE),
            List.of(new TopNOperator.SortOrder(0, asc, false)),
            pageRows,
            Long.MAX_VALUE,
            TopNOperator.InputOrdering.NOT_SORTED,
            null,
            parallel
        );
    }

    private static List<Page> input(BlockFactory blockFactory, long[] keys, BytesRef[] values, int pageSize) {
        List<Page> pages = new ArrayList<>();
        for (int start = 0; start < keys.length; start += pageSize) {
            int end = Math.min(keys.length, start + pageSize);
            try (
                LongBlock.Builder keyBuilder = blockFactory.newLongBlockBuilder(end - start);
                BytesRefBlock.Builder valueBuilder = blockFactory.newBytesRefBlockBuilder(end - start)
            ) {
                for (int i = start; i < end; i++) {
                    keyBuilder.appendLong(keys[i]);
                    valueBuilder.appendBytesRef(values[i]);
                }
                pages.add(new Page(keyBuilder.build(), valueBuilder.build()));
            }
        }
        return pages;
    }

    private static void assertRows(List<Page> expected, List<Page> actual) {
        assertThat(rows(actual), equalTo(rows(expected)));
    }

    private static List<String> rows(List<Page> pages) {
        List<String> rows = new ArrayList<>();
        for (Page page : pages) {
            LongBlock keys = page.getBlock(0);
            BytesRefBlock values = page.getBlock(1);
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                rows.add(keys.getLong(p) + "|" + values.getBytesRef(p, scratch));
            }
        }
        return rows;
    }

    private static long[] uniqueKeys(int rows) {
        List<Long> keys = new ArrayList<>(rows);
        for (long i = 0; i < rows; i++) {
            keys.add(i);
        }
        Collections.shuffle(keys, random());
        long[] array = new long[rows];
        for (int i = 0; i < rows; i++) {
            array[i] = keys.get(i);
        }
        return array;
    }

    private static BytesRef[] randomValues(int rows, int minLength, int maxLength) {
        BytesRef[] values = new BytesRef[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = new BytesRef(randomByteArrayOfLength(between(minLength, maxLength)));
        }
        return values;
    }

    /**
     * Moves a blocked operator along: a held completion is released when the store holds one, else the workers get a
     * moment.
     */
    private static void nudge(MemorySpillStore store, Operator op) throws Exception {
        if (store.heldOperations() > 0) {
            assertTrue(store.releaseRandom());
        } else if (op.isBlocked().listener().isDone() == false) {
            Thread.sleep(1);
        } else {
            Thread.yield();
        }
    }

    private static void awaitHeld(MemorySpillStore store) throws Exception {
        assertBusy(() -> assertThat("the store holds an operation", store.heldOperations(), greaterThan(0)));
    }

    /**
     * Releases what the store holds until {@code done} completes: an operation may still be on its way from a pool
     * thread when the store holds nothing, so the wait is a deadline rather than a number of rounds.
     */
    private static void releaseUntilDone(MemorySpillStore store, PlainActionFuture<Void> done) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (done.isDone() == false) {
            if (store.releaseAll() > 0) {
                continue;
            }
            assertTrue("the store's operations did not complete in time", System.nanoTime() < deadline);
            Thread.sleep(1);
        }
        done.get();
    }
}
