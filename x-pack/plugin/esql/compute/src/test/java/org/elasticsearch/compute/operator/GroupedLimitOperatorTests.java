/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class GroupedLimitOperatorTests extends OperatorTestCase {

    private TestThreadPool threadPool;

    @Before
    public void startCodecPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopCodecPool() throws Exception {
        // the scopes complete their close on this pool, so they go before it; the base class's hook then finds none
        closeSpillingScopes();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    @Override
    protected GroupedLimitOperator.Factory simple(SimpleOptions options) {
        return new GroupedLimitOperator.Factory(100, List.of(0), List.of(ElementType.LONG));
    }

    @Override
    protected boolean supportsSpilling() {
        return true;
    }

    @Override
    protected boolean spillChangesOutputOrder() {
        return true;
    }

    private MemorySpillStore store(MemorySpillStore.Completion completion) {
        // the store's own random: its reads complete on the pool's threads
        return new MemorySpillStore(completion, threadPool.generic(), Long.MAX_VALUE, "test.spill", new Random(randomLong()));
    }

    /**
     * The codec pool, apart from the pool the store completes on: a codec task waits for an append's completion, which
     * must not need the thread it parks.
     */
    private Executor codec() {
        return threadPool.executor(ThreadPool.Names.SEARCH);
    }

    /**
     * Waits for the codec thread to hand the store an operation, which the store then holds.
     */
    private static void awaitHeld(MemorySpillStore store) throws Exception {
        assertBusy(() -> assertThat("the store holds an operation", store.heldOperations(), greaterThan(0)));
    }

    private static List<Long> longs(Page page, int channel) {
        LongBlock block = page.getBlock(channel);
        List<Long> values = new ArrayList<>();
        for (int p = 0; p < block.getPositionCount(); p++) {
            values.add(block.getLong(block.getFirstValueIndex(p)));
        }
        return values;
    }

    private static Page longPage(BlockFactory blockFactory, long... values) {
        List<Object> boxed = new ArrayList<>();
        for (long value : values) {
            boxed.add(value);
        }
        return new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, boxed));
    }

    /** Shard references that count what is taken and given back: what a source hands its doc vectors. */
    private static final class CountingShards implements IndexedByShardId<RefCounted> {
        final List<AbstractRefCounted> shards = new ArrayList<>();

        CountingShards(int count) {
            for (int i = 0; i < count; i++) {
                shards.add(AbstractRefCounted.of(() -> {}));
            }
        }

        @Override
        public RefCounted get(int shardId) {
            return shards.get(shardId);
        }

        @Override
        public Iterable<? extends RefCounted> iterable() {
            return shards;
        }

        @Override
        public int size() {
            return shards.size();
        }

        @Override
        public <S> IndexedByShardId<S> map(Function<RefCounted, S> mapper) {
            throw new UnsupportedOperationException();
        }

        int references(int shard) {
            return shards.get(shard).refCount();
        }
    }

    private static Page docPage(BlockFactory blockFactory, CountingShards shards, int[] shardOfRow, long[] keys) {
        int size = shardOfRow.length;
        int[] segments = new int[size];
        int[] docs = new int[size];
        for (int i = 0; i < size; i++) {
            docs[i] = i;
        }
        DocVector vector = new DocVector(
            shards,
            blockFactory.newIntArrayVector(shardOfRow, size),
            blockFactory.newIntArrayVector(segments, size),
            blockFactory.newIntArrayVector(docs, size),
            DocVector.config()
        );
        return new Page(vector.asBlock(), blockFactory.newLongArrayVector(keys, size).asBlock());
    }

    /**
     * Spilled rows that reference documents keep their shards alive: the operator holds a reference on every shard
     * they name from the spill to its close, the pages read back and the output take their own, and all go back.
     */
    public void testSpilledDocumentsKeepTheirShardsPinned() {
        MemorySpillStore store = store(MemorySpillStore.Completion.INLINE);
        DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        BlockFactory blockFactory = ctx.blockFactory();
        CountingShards shards = new CountingShards(2);
        List<Page> output = new ArrayList<>();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator.Factory(1, List.of(1), List.of(ElementType.DOC, ElementType.LONG)).get(ctx)
        ) {
            // every row spills at the first page: keys 1, 2, 1 on shards 0, 1, 0
            op.addInput(docPage(blockFactory, shards, new int[] { 0, 1, 0 }, new long[] { 1, 2, 1 }));
            assertThat(op.getOutput(), nullValue());
            assertThat("the page's own references went with it, the operator's pin stays", shards.references(0), equalTo(2));
            assertThat(shards.references(1), equalTo(2));
            op.finish();
            for (int i = 0; i < 1000 && op.isFinished() == false; i++) {
                Page page = op.getOutput();
                if (page != null) {
                    output.add(page);
                }
            }
            assertTrue(op.isFinished());
            assertThat(
                "one row per key came back, each holding its shard",
                output.stream().mapToInt(Page::getPositionCount).sum(),
                equalTo(2)
            );
            assertThat(shards.references(0), equalTo(3));
            assertThat(shards.references(1), equalTo(3));
        }
        assertThat("the pins went with the operator", shards.references(0), equalTo(2));
        assertThat(shards.references(1), equalTo(2));
        output.forEach(Page::releaseBlocks);
        assertThat(shards.references(0), equalTo(1));
        assertThat(shards.references(1), equalTo(1));
    }

    /**
     * Keys seen before the freeze keep streaming, in order, with their counts; keys first seen after it come back from
     * the replay at the end, with the first {@code limit} rows of each in arrival order; nothing is emitted twice.
     */
    public void testFrozenKeysStreamAndNewKeysReplay() {
        MemorySpillStore store = store(MemorySpillStore.Completion.INLINE);
        DriverContext ctx = spillingDriverContext(ByteSizeValue.ofGb(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        BlockFactory blockFactory = ctx.blockFactory();
        List<Page> output = new ArrayList<>();
        try (GroupedLimitOperator op = new GroupedLimitOperator.Factory(2, List.of(0), List.of(ElementType.LONG)).get(ctx)) {
            op.addInput(longPage(blockFactory, 1, 2, 1));
            output.add(op.getOutput());
            assertThat(longs(output.get(0), 0), equalTo(List.of(1L, 2L, 1L)));

            // the next page crosses the budget: 1 and 2 are frozen, 3 and 4 are new
            ctx.queryScope().breaker().setBudget(0);
            op.addInput(longPage(blockFactory, 3, 1, 2, 4, 3, 2));
            Page streamed = op.getOutput();
            assertThat("the frozen key 2 streams, 1 is full, 3 and 4 went to disk", longs(streamed, 0), equalTo(List.of(2L)));
            output.add(streamed);
            assertThat(op.status().rowsSpilled(), equalTo(3L));

            op.addInput(longPage(blockFactory, 4, 3, 5));
            assertThat("nothing streams from a page of new keys", op.getOutput(), nullValue());
            // the replay runs with the whole budget back, so no partition freezes again
            ctx.queryScope().breaker().setBudget(ByteSizeValue.ofGb(1).getBytes());
            op.finish();
            assertFalse(op.isFinished());
            List<Long> replayed = new ArrayList<>();
            for (int i = 0; i < 1000 && op.isFinished() == false; i++) {
                assertTrue("inline completion: never blocked", op.isBlocked().listener().isDone());
                Page page = op.getOutput();
                if (page != null) {
                    output.add(page);
                    replayed.addAll(longs(page, 0));
                }
            }
            assertTrue(op.isFinished());
            assertThat(
                "two of each new key, arrival order kept within a key",
                replayed.stream().sorted().toList(),
                equalTo(List.of(3L, 3L, 4L, 4L, 5L))
            );
            assertThat(op.status().rowsSpilled(), equalTo(6L));
            assertThat(op.status().partitionsReplayed(), greaterThan(0));
            assertThat(op.status().spill().spillRuns(), equalTo(1));
            assertThat(op.status().spill().spilledBytes(), greaterThan(0L));
            assertThat(Strings.toString(op.status()), containsString("\"rows_spilled\":6"));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
        assertThat(store.liveScopes(), equalTo(1));
    }

    /**
     * Multivalued, null and large keys, under a budget that makes every operator freeze at once, so the replay freezes
     * again and partitions with another seed, down to the depth where it stops spilling: the output is what the
     * resident operator produces, as a multiset, and it comes in several pages.
     */
    public void testMultivaluesNullsAndLargeKeysThroughRecursivePartitions() {
        DriverContext inputContext = driverContext();
        List<Page> input = new ArrayList<>();
        int pages = between(5, 20);
        for (int i = 0; i < pages; i++) {
            int positions = between(1, 60);
            try (
                LongBlock.Builder longs = inputContext.blockFactory().newLongBlockBuilder(positions);
                BytesRefBlock.Builder bytes = inputContext.blockFactory().newBytesRefBlockBuilder(positions)
            ) {
                for (int p = 0; p < positions; p++) {
                    switch (between(0, 3)) {
                        case 0 -> longs.appendNull();
                        case 1 -> longs.appendLong(between(0, 50));
                        case 2, 3 -> {
                            longs.beginPositionEntry();
                            int count = between(1, 3);
                            for (int v = 0; v < count; v++) {
                                longs.appendLong(between(0, 5));
                            }
                            longs.endPositionEntry();
                        }
                        default -> throw new AssertionError("unexpected case");
                    }
                    if (rarely()) {
                        bytes.appendNull();
                    } else {
                        bytes.appendBytesRef(
                            new BytesRef(randomAlphaOfLength(between(1, 3)) + (rarely() ? randomAlphaOfLength(5000) : ""))
                        );
                    }
                }
                input.add(new Page(longs.build(), bytes.build()));
            }
        }
        List<Page> origInput = CannedSourceOperator.deepCopyOf(inputContext.blockFactory(), input);
        GroupedLimitOperator.Factory factory = new GroupedLimitOperator.Factory(
            between(1, 3),
            List.of(0, 1),
            List.of(ElementType.LONG, ElementType.BYTES_REF)
        );
        DriverContext residentContext = driverContext();
        List<Page> resident = new TestDriverRunner().builder(residentContext).input(origInput).run(factory.get(residentContext));

        MemorySpillStore store = store(randomFrom(MemorySpillStore.Completion.INLINE, MemorySpillStore.Completion.EXECUTOR));
        DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, codec());
        GroupedLimitOperator op = factory.get(ctx);
        // every row goes through the maximum depth of runs here, one record per run at the bottom: slow by design
        List<Page> results = new TestDriverRunner().timeout(TimeValue.timeValueMinutes(3)).builder(ctx).input(input).run(op);
        try {
            assertThat(rows(results, true), equalTo(rows(resident, true)));
            GroupedLimitOperator.Status status = op.status();
            assertThat("every page froze at once", status.rowsSpilled(), greaterThan(0L));
            assertThat("the replay froze again", status.spill().spillRuns(), greaterThan(1));
            assertThat(status.partitionsReplayed(), greaterThan(0));
            if (resident.stream().mapToInt(Page::getPositionCount).sum() > 100) {
                assertThat("the replay emits several pages", results.size(), greaterThan(1));
            }
        } finally {
            results.forEach(Page::releaseBlocks);
            resident.forEach(Page::releaseBlocks);
        }
    }

    /**
     * With a store that holds every completion, the operator reports blocked rather than spinning: the headroom, the
     * appends, the finish, the open and every read complete only when the test releases them, in random order, and
     * the output is still right.
     */
    public void testADelayedStoreBlocksTheOperator() throws Exception {
        MemorySpillStore store = store(MemorySpillStore.Completion.DEFERRED);
        DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, codec());
        BlockFactory blockFactory = ctx.blockFactory();
        List<Page> output = new ArrayList<>();
        List<Long> seen = new ArrayList<>();
        try (GroupedLimitOperator op = new GroupedLimitOperator.Factory(1, List.of(0), List.of(ElementType.LONG)).get(ctx)) {
            op.addInput(longPage(blockFactory, 1, 2, 1));
            assertFalse("the page waits for the writer", op.needsInput());
            assertFalse(op.isBlocked().listener().isDone());
            assertThat(store.releaseAll(), greaterThan(0));
            assertTrue(op.isBlocked().listener().isDone());
            assertThat(op.getOutput(), nullValue());
            op.finish();
            int rounds = 0;
            while (op.isFinished() == false) {
                assertThat("the operator makes progress", ++rounds, lessThanOrEqualTo(10_000));
                if (op.isBlocked().listener().isDone() == false) {
                    // what the operator waits for is either held already or on its way from the codec thread
                    awaitHeld(store);
                    assertTrue("blocked on a held completion", store.releaseRandom());
                    continue;
                }
                Page page = op.getOutput();
                if (page != null) {
                    output.add(page);
                    seen.addAll(longs(page, 0));
                }
            }
            assertThat(seen.stream().sorted().toList(), equalTo(List.of(1L, 2L)));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Closed with a write in flight, as a cancelled driver closes it: the abort fails what was queued, the pending
     * operations complete, the driver's wait ends and everything borrowed goes back.
     */
    public void testCloseWithAPendingWriteReleasesEverything() throws Exception {
        MemorySpillStore store = store(MemorySpillStore.Completion.DEFERRED);
        DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, codec());
        BlockFactory blockFactory = ctx.blockFactory();
        GroupedLimitOperator op = new GroupedLimitOperator.Factory(1, List.of(0), List.of(ElementType.LONG)).get(ctx);
        op.addInput(longPage(blockFactory, 1, 2, 3));
        assertThat("the writer is being created", store.releaseAll(), greaterThan(0));
        assertThat(op.getOutput(), nullValue());
        awaitHeld(store);
        op.close();
        ctx.finish();
        PlainActionFuture<Void> quiet = new PlainActionFuture<>();
        ctx.waitForAsyncActions(quiet);
        for (int i = 0; i < 1000 && quiet.isDone() == false; i++) {
            store.releaseAll();
            Thread.yield();
        }
        quiet.get(30, TimeUnit.SECONDS);
        assertThat(store.liveScopes(), equalTo(1));
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("GroupedLimitOperator[limitPerGroup = 100]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("GroupedLimitOperator[limitPerGroup = 100, groupKeys = [0], groups = 0]");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int inputPositionCount = input.stream().mapToInt(Page::getPositionCount).sum();
        int outputPositionCount = results.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(outputPositionCount, equalTo(inputPositionCount));
    }

    public void testStatus() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (GroupedLimitOperator op = simple(SimpleOptions.DEFAULT).get(ctx)) {
            GroupedLimitOperator.Status status = op.status();
            assertThat(status.pagesProcessed(), equalTo(0));
            assertThat(status.rowsReceived(), equalTo(0L));
            assertThat(status.rowsEmitted(), equalTo(0L));

            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 1L)));
            op.addInput(p);
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(3));
            } finally {
                output.releaseBlocks();
            }

            status = op.status();
            assertThat(status.pagesProcessed(), equalTo(1));
            assertThat(status.rowsReceived(), equalTo(3L));
            assertThat(status.rowsEmitted(), equalTo(3L));
        }
    }

    public void testNeedInput() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (GroupedLimitOperator op = simple(SimpleOptions.DEFAULT).get(ctx)) {
            assertTrue(op.needsInput());
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 3L)));
            op.addInput(p);
            assertFalse(op.needsInput());
            op.getOutput().releaseBlocks();
            assertTrue(op.needsInput());
            op.finish();
            assertFalse(op.needsInput());
        }
    }

    /**
     * With limit=2, group 1 appears 3 times and group 2 appears 2 times.
     * Only the first 2 rows of group 1 should be retained.
     */
    public void testLimitPerGroupSinglePage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                2,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 1L, 1L, 2L)));
            op.addInput(p);
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(4));
                LongBlock resultBlock = output.getBlock(0);
                assertThat(resultBlock.getLong(0), equalTo(1L));
                assertThat(resultBlock.getLong(1), equalTo(2L));
                assertThat(resultBlock.getLong(2), equalTo(1L));
                assertThat(resultBlock.getLong(3), equalTo(2L));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    /**
     * Group limits are enforced across page boundaries: a group saturated
     * in the first page rejects further rows in subsequent pages.
     */
    public void testLimitPerGroupAcrossPages() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                2,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p1 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L, 2L)));
            op.addInput(p1);
            Page out1 = op.getOutput();
            try {
                assertThat(out1.getPositionCount(), equalTo(3));
            } finally {
                out1.releaseBlocks();
            }

            Page p2 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 2L)));
            op.addInput(p2);
            Page out2 = op.getOutput();
            try {
                assertThat(out2.getPositionCount(), equalTo(1));
                LongBlock b = out2.getBlock(0);
                assertThat(b.getLong(0), equalTo(2L));
            } finally {
                out2.releaseBlocks();
            }
        }
    }

    /**
     * When every row in a page belongs to an already-saturated group,
     * the entire page is dropped and getOutput returns null.
     */
    public void testEntirePageFiltered() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                1,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p1 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L)));
            op.addInput(p1);
            Page out1 = op.getOutput();
            try {
                assertThat(out1.getPositionCount(), equalTo(1));
            } finally {
                out1.releaseBlocks();
            }

            Page p2 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L, 1L)));
            op.addInput(p2);
            assertThat(op.getOutput(), nullValue());
        }
    }

    public void testPagePassesThroughWhenAllAccepted() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                10,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            );
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 3L)));
        ) {
            op.addInput(p.shallowCopy());
            try (Page output = op.getOutput()) {
                for (int b = 0; b < output.getBlockCount(); b++) {
                    assertThat(output.getBlock(b), sameInstance(p.getBlock(b)));
                }
            }
        }
    }

    /**
     * With limitPerGroup=0 every row is dropped regardless of the group key.
     * No keys should be tracked and status must reflect zero emitted rows.
     */
    public void testLimitPerGroupZero() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                0,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p1 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 3L)));
            op.addInput(p1);
            assertThat(op.getOutput(), nullValue());

            Page p2 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L, 2L)));
            op.addInput(p2);
            assertThat(op.getOutput(), nullValue());

            GroupedLimitOperator.Status status = op.status();
            assertThat(status.rowsReceived(), equalTo(6L));
            assertThat(status.rowsEmitted(), equalTo(0L));
            assertThat(status.pagesProcessed(), equalTo(0));
            assertThat(status.groupCount(), equalTo(0));
        }
    }

    /**
     * Groups are determined by composite keys across multiple channels.
     * (1,10), (1,20), and (2,10) are three distinct groups.
     */
    public void testMultipleGroupChannels() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                1,
                groupKeyEncoder(blockFactory, new int[] { 0, 1 }, List.of(ElementType.LONG, ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p = new Page(
                BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L, 1L, 2L)),
                BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(10L, 20L, 10L, 10L))
            );
            op.addInput(p);
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(3));
                LongBlock col0 = output.getBlock(0);
                LongBlock col1 = output.getBlock(1);
                assertThat(col0.getLong(0), equalTo(1L));
                assertThat(col1.getLong(0), equalTo(10L));
                assertThat(col0.getLong(1), equalTo(1L));
                assertThat(col1.getLong(1), equalTo(20L));
                assertThat(col0.getLong(2), equalTo(2L));
                assertThat(col1.getLong(2), equalTo(10L));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    /**
     * A multivalue {@code [1,2]} and a single value {@code 1} are different groups.
     */
    public void testMultivalueAndSingleValueAreDifferentGroups() {
        assertDistinctGroups(2, List.of(1L, 2L), 1L);
    }

    /**
     * A null position and a multivalue {@code [1,2]} are different groups.
     */
    public void testNullAndMultivalueAreDifferentGroups() {
        assertDistinctGroups(2, null, List.of(1L, 2L));
    }

    /**
     * Two identical multivalues {@code [1,2]} belong to the same group.
     */
    public void testIdenticalMultivaluesAreSameGroup() {
        assertDistinctGroups(1, List.of(1L, 2L), List.of(1L, 2L));
    }

    /**
     * {@code [1,2]} and {@code [2,1]} are different groups because
     * multivalues use order-sensitive list semantics.
     */
    public void testMultivaluesWithDifferentOrderAreDifferentGroups() {
        assertDistinctGroups(2, List.of(1L, 2L), List.of(2L, 1L));
    }

    /**
     * A single-element multivalue {@code [1]} and a scalar {@code 1} are the
     * same group because the block representation is identical (valueCount=1).
     */
    public void testSingleElementMultivalueAndScalarAreSameGroup() {
        assertDistinctGroups(1, 1L, 1L);
    }

    /**
     * A multivalue with duplicates {@code [1,1]} and a single value {@code 1}
     * are different groups because the value count differs.
     */
    public void testMultivalueWithDuplicatesAndSingleValueAreDifferentGroups() {
        assertDistinctGroups(2, List.of(1L, 1L), 1L);
    }

    /**
     * Two identical multivalues with duplicates {@code [1,1]} are the same group.
     */
    public void testIdenticalMultivaluesWithDuplicatesAreSameGroup() {
        assertDistinctGroups(1, List.of(1L, 1L), List.of(1L, 1L));
    }

    /**
     * Multivalues of different lengths sharing a prefix are different groups:
     * {@code [1,2,3]} vs {@code [1,2]}.
     */
    public void testDifferentLengthMultivaluesAreDifferentGroups() {
        assertDistinctGroups(2, List.of(1L, 2L, 3L), List.of(1L, 2L));
    }

    /**
     * Multivalue semantics apply within composite keys: {@code ([1,2], 10)},
     * {@code ([1,2], 20)}, {@code ([2,1], 10)} and {@code ([2,1], 10)} are three distinct groups.
     */
    public void testMultivalueInCompositeKey() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        Page p = new Page(
            BlockTestUtils.asBlock(
                blockFactory,
                ElementType.LONG,
                List.of(List.of(1L, 2L), List.of(1L, 2L), List.of(2L, 1L), List.of(2L, 1L))
            ),
            BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(10L, 20L, 10L, 10L))
        );
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                1,
                groupKeyEncoder(blockFactory, new int[] { 0, 1 }, List.of(ElementType.LONG, ElementType.LONG)),
                blockFactory
            )
        ) {
            op.addInput(p);
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(3));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    /**
     * Two null positions belong to the same group.
     */
    public void testTwoNullsAreSameGroup() {
        assertDistinctGroups(1, null, null);
    }

    /**
     * Feeds the given values through a {@code limit=1} single-channel operator
     * and asserts the number of output positions (i.e. distinct groups).
     * Each entry can be: {@code null} for a null position, a {@code Long} scalar,
     * or a {@code List<Long>} for a multivalue.
     */
    private void assertDistinctGroups(int expectedGroups, Object... values) {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                1,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            op.addInput(new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, Arrays.asList(values))));
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(expectedGroups));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    private static GroupKeyEncoder groupKeyEncoder(BlockFactory blockFactory, int[] groupChannels, List<ElementType> elementTypes) {
        return new GroupKeyEncoder(
            groupChannels,
            elementTypes,
            new PagedBytesBuilder(blockFactory.bigArrays().recycler(), blockFactory.breaker(), "group-key-encoder", 64)
        );
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        var emittedRows = output.stream().mapToInt(Page::getPositionCount).sum();
        var inputRows = input.stream().mapToInt(Page::getPositionCount).sum();

        var mapMatcher = matchesMap().entry("limit_per_group", 100)
            .entry("group_count", greaterThanOrEqualTo(0))
            .entry("pages_processed", output.size())
            .entry("rows_received", allOf(greaterThanOrEqualTo(emittedRows), lessThanOrEqualTo(inputRows)))
            .entry("rows_emitted", emittedRows)
            .entry("ram_bytes_used", greaterThanOrEqualTo(0))
            .entry("ram_used", notNullValue());
        if (map.containsKey("rows_spilled")) {
            // present by value: only a run that froze reports its spill
            mapMatcher = mapMatcher.entry("rows_spilled", greaterThan(0))
                .entry("partitions_replayed", greaterThan(0))
                .entry("spilled_bytes", greaterThan(0))
                .entry("spilled", notNullValue())
                .entry("read_back_bytes", greaterThan(0))
                .entry("read_back", notNullValue())
                .entry("spill_runs", greaterThan(0))
                .entry("spill_nanos", greaterThanOrEqualTo(0));
        }

        assertMap(map, mapMatcher);
    }
}
