/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.LongIntBlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.ParallelHashAggregationOperator;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
import org.elasticsearch.compute.spill.FramedRunLayout;
import org.elasticsearch.compute.spill.FramedRunMaxima;
import org.elasticsearch.compute.spill.FramedRunReader;
import org.elasticsearch.compute.spill.FramedRunWriter;
import org.elasticsearch.compute.spill.SliceRunReader;
import org.elasticsearch.compute.spill.SliceRunWriter;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests grouping aggregations with and without partitioning.
 * Partitioned block hashes are covered by {@link org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHashTestCase}
 */
public abstract class PartitionedGroupingAggregatorFunctionTestCase extends GroupingAggregatorFunctionTestCase {
    private static final String WORKER_EXECUTOR = "esql_partition_worker";

    private int longKeyChannel = -1;
    private int intKeyChannel = -1;
    private int tsidChannel = -1;
    private int timestampChannel = -1;

    /**
     * Checks that partitioning does not change aggregation results.
     */
    public final void testPartitioning() {
        assumeTrue("partitioned block hashes are not supported on this platform", PartitionedBlockHash.supportPartitioning());
        DriverContext driverContext = driverContext();
        try (var agg = aggregatorFunction().groupingAggregator(driverContext, channels(AggregatorMode.SINGLE))) {
            assumeTrue("aggregator [" + agg + "] doesn't support partitioning", agg.supportPartitioning());
        }
        var pages = pagesWithKeys(driverContext.blockFactory());
        try {
            List<AggResult> singlePassResult = runAggregation(driverContext(), pages, Integer.MAX_VALUE);
            assertThat(singlePassResult, not(empty()));
            List<AggResult> partitioningResult = runAggregation(driverContext(), pages, between(128, 1024));
            assertAggregatedResults(singlePassResult, partitioningResult);
        } finally {
            Releasables.close(pages);
        }
    }

    /**
     * Checks that partitioning releases all resources when a cranky breaker trips midway
     */
    public final void testPartitioningWithCrankyBreaker() {
        assumeTrue("partitioned block hashes are not supported on this platform", PartitionedBlockHash.supportPartitioning());
        DriverContext driverContext = driverContext();
        try (var agg = aggregatorFunction().groupingAggregator(driverContext, channels(AggregatorMode.SINGLE))) {
            assumeTrue("aggregator [" + agg + "] doesn't support partitioning", agg.supportPartitioning());
        }
        var pages = pagesWithKeys(driverContext.blockFactory());
        try {
            List<AggResult> partitioningResult = runAggregation(crankyDriverContext(), pages, between(128, 1024));
            assertThat(partitioningResult, not(empty()));
            // if succeeds, then the result should be the same as single pass
            List<AggResult> singlePassResult = runAggregation(driverContext(), pages, Integer.MAX_VALUE);
            assertAggregatedResults(singlePassResult, partitioningResult);
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), containsString("cranky breaker"));
        } finally {
            Releasables.close(pages);
        }
    }

    /**
     * Checks that spilling does not change aggregation results: the parallel operator runs under a budget of a few
     * kilobytes, so every generation goes to a slice run and comes back through the codecs.
     */
    public final void testPartitioningWithSpilling() throws Exception {
        assumeTrue("partitioned block hashes are not supported on this platform", PartitionedBlockHash.supportPartitioning());
        DriverContext driverContext = driverContext();
        try (var agg = aggregatorFunction().groupingAggregator(driverContext, channels(AggregatorMode.SINGLE))) {
            assumeTrue("aggregator [" + agg + "] doesn't support partitioning", agg.supportPartitioning());
            assumeTrue("aggregator [" + agg + "] has no partition codec", agg.supportsPartitionCodec());
        }
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.INLINE,
            null,
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        var pages = pagesWithKeys(driverContext.blockFactory());
        try {
            List<AggResult> singlePassResult = runAggregation(driverContext(), pages, Integer.MAX_VALUE);
            assertThat(singlePassResult, not(empty()));
            DriverContext spilling = spillingDriverContext(
                ByteSizeValue.ofKb(between(1, 16)),
                0,
                store,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            List<AggResult> spilledResult = runAggregation(spilling, pages, between(128, 1024));
            assertAggregatedResults(singlePassResult, spilledResult);
        } finally {
            Releasables.close(pages);
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            shutdown.get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Checks the slice route of a spilled generation: every partition of every generation is written with the key and
     * state codecs into a slice run, restored from it and combined, and the result must match the single pass.
     */
    public final void testPartitionCodec() {
        assumeTrue("partitioned block hashes are not supported on this platform", PartitionedBlockHash.supportPartitioning());
        DriverContext driverContext = driverContext();
        try (var agg = aggregatorFunction().groupingAggregator(driverContext, channels(AggregatorMode.SINGLE))) {
            assumeTrue("aggregator [" + agg + "] doesn't support partitioning", agg.supportPartitioning());
            assumeTrue("aggregator [" + agg + "] has no partition codec", agg.supportsPartitionCodec());
        }
        var pages = pagesWithKeys(driverContext.blockFactory());
        try {
            List<AggResult> singlePassResult = runAggregation(driverContext(), pages, Integer.MAX_VALUE);
            assertThat(singlePassResult, not(empty()));
            List<AggResult> sliceResult = runAggregationThroughSliceRuns(driverContext(), pages);
            assertAggregatedResults(singlePassResult, sliceResult);
        } finally {
            Releasables.close(pages);
        }
    }

    public final void testTimeSeriesPartitioning() {
        assumeTrue("partitioned block hashes are not supported on this platform", PartitionedBlockHash.supportPartitioning());
        runTimeSeriesPartitioningTest(aggregatorFunction());
    }

    public final void testTimeSeriesPartitioningWithWindow() {
        assumeTrue("partitioned block hashes are not supported on this platform", PartitionedBlockHash.supportPartitioning());
        runTimeSeriesPartitioningTest(new WindowAggregatorFunctionSupplier(aggregatorFunction(), Duration.ofMinutes(between(1, 5))));
    }

    List<Page> pagesWithKeys(BlockFactory blockFactory) {
        List<Page> pages = CannedSourceOperator.collectPages(simpleInput(blockFactory, between(1, 20_000)));
        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            if (longKeyChannel == -1) {
                longKeyChannel = page.getBlockCount();
                intKeyChannel = longKeyChannel + 1;
            } else if (longKeyChannel != page.getBlockCount()) {
                throw new AssertionError("expected [" + longKeyChannel + "] blocks; got [" + page.getBlockCount());
            }
            Block[] blocks = new Block[page.getBlockCount() + 2];
            for (int b = 0; b < page.getBlockCount(); b++) {
                blocks[b] = page.getBlock(b);
            }
            blocks[longKeyChannel] = longKeyBlock(blockFactory, page.getPositionCount());
            blocks[intKeyChannel] = intKeyBlock(blockFactory, page.getPositionCount());
            pages.set(i, new Page(blocks));
        }
        return pages;
    }

    protected void assertAggregatedResults(List<AggResult> singlePassResult, List<AggResult> partitioningResult) {
        assertThat(partitioningResult, equalTo(singlePassResult));
    }

    record Key(Long longKey, Integer intKey) implements Comparable<Key> {
        private static final Comparator<Key> COMPARATOR = Comparator.comparing(Key::longKey, Comparator.nullsFirst(Long::compare))
            .thenComparing(Key::intKey, Comparator.nullsFirst(Integer::compare));

        @Override
        public int compareTo(Key other) {
            return COMPARATOR.compare(this, other);
        }
    }

    record AggResult(Key key, Object value) implements Comparable<AggResult> {
        @Override
        public int compareTo(AggResult other) {
            return key.compareTo(other.key);
        }
    }

    List<AggResult> runAggregation(DriverContext driverContext, List<Page> inputPages, int partitionKeysThreshold) {
        final TestThreadPool threadPool = newWorkerThreadPool();
        final List<Page> outputPages = new ArrayList<>();
        HashAggregationOperator hashOperator = null;
        try {
            var parallelConfig = parallelConfig(threadPool, partitionKeysThreshold);
            hashOperator = new HashAggregationOperator(
                AggregatorMode.SINGLE,
                List.of(aggregatorFunction().groupingAggregatorFactory(AggregatorMode.SINGLE, channels(AggregatorMode.SINGLE))),
                newBlockHash(),
                randomIntBetween(1, 1024),
                randomDouble(),
                randomIntBetween(128, 4096),
                null,
                driverContext,
                parallelConfig,
                randomBoolean()
            );
            try (
                var source = new CannedSourceOperator(CannedSourceOperator.deepCopyOf(driverContext.blockFactory(), inputPages).iterator());
                Driver driver = TestDriverFactory.create(
                    driverContext,
                    source,
                    List.of(hashOperator),
                    new TestResultPageSinkOperator(outputPages::add)
                )
            ) {
                hashOperator = null;
                new TestDriverRunner().run(driver);
            }
            return rows(outputPages);
        } finally {
            Releasables.close(hashOperator);
            Releasables.close(outputPages);
            terminate(threadPool);
        }
    }

    private static List<AggResult> rows(List<Page> outputPages) {
        List<AggResult> rows = new ArrayList<>();
        for (Page page : outputPages) {
            assertThat(page.getBlockCount(), equalTo(3));
            LongBlock longKeys = page.getBlock(0);
            IntBlock intKeys = page.getBlock(1);
            Block valueBlock = page.getBlock(2);
            for (int p = 0; p < page.getPositionCount(); p++) {
                Key key = new Key(longKeys.isNull(p) ? null : longKeys.getLong(p), intKeys.isNull(p) ? null : intKeys.getInt(p));
                rows.add(new AggResult(key, BlockUtils.toJavaObject(valueBlock, p)));
            }
        }
        return rows.stream().sorted().toList();
    }

    /**
     * The slice route by hand, as {@link ParallelHashAggregationOperator} runs it under spilling: generations are split,
     * each written to its own slice run, then every partition is restored from every run and combined into fresh
     * state. The store completes inline, so the codec and the reads run on this thread. One aggregator never sees input
     * and only decodes, as the operator's decoder does.
     */
    private List<AggResult> runAggregationThroughSliceRuns(DriverContext driverContext, List<Page> inputPages) {
        CircuitBreaker breaker = driverContext.breaker();
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.INLINE,
            null,
            Long.MAX_VALUE,
            "test.limit",
            new Random(randomLong())
        );
        SpillScope scope = store.newScope("q", breaker);
        Function<DriverContext, GroupingAggregator> factory = aggregatorFunction().groupingAggregatorFactory(
            AggregatorMode.SINGLE,
            channels(AggregatorMode.SINGLE)
        );
        List<Page> pages = CannedSourceOperator.deepCopyOf(driverContext.blockFactory(), inputPages);
        List<SliceRunReader> runs = new ArrayList<>();
        List<Page> outputPages = new ArrayList<>();
        PartitionedBlockHash hash = (PartitionedBlockHash) newBlockHash().apply(driverContext);
        GroupingAggregator decoder = factory.apply(driverContext);
        GroupingAggregator aggregator = factory.apply(driverContext);
        try {
            int pagesPerGeneration = between(1, pages.size());
            for (int i = 0; i < pages.size(); i++) {
                Page page = pages.get(i);
                GroupingAggregatorFunction.AddInput prepared = aggregator.prepareProcessPage(hash, page);
                try (GroupingAggregatorFunction.AddInput add = prepared == null ? NO_INPUT : prepared) {
                    hash.add(page, add);
                }
                if (((i + 1) % pagesPerGeneration == 0 || i == pages.size() - 1) && hash.numKeys() > 0) {
                    runs.add(spillGeneration(scope, breaker, hash, aggregator.aggregatorFunction(), decoder.aggregatorFunction()));
                    hash.clear();
                    aggregator.close();
                    aggregator = factory.apply(driverContext);
                }
            }
            int[][] ids = new int[runs.size()][];
            boolean[] appendOnly = new boolean[runs.size()];
            for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                final int partition = p;
                hash.clear();
                aggregator.close();
                aggregator = factory.apply(driverContext);
                GroupingAggregatorFunction function = aggregator.aggregatorFunction();
                List<SliceRunReader.Slice> slices = new ArrayList<>(runs.size());
                try {
                    for (SliceRunReader run : runs) {
                        slices.add(await(l -> run.readSlice(partition, l)));
                    }
                    for (int g = 0; g < slices.size(); g++) {
                        SliceRunReader.Slice slice = slices.get(g);
                        if (slice.isEmpty()) {
                            continue;
                        }
                        assertThat(slice.keys().keysInPartition(partition), equalTo(slice.keyCount()));
                        assertThat(slice.states().length, equalTo(1));
                        if (ids[g] == null || ids[g].length < slice.keyCount()) {
                            ids[g] = new int[slice.keyCount()];
                        }
                        appendOnly[g] = hash.combinePartition(slice.keys(), partition, ids[g]);
                    }
                    for (SliceRunReader.Slice slice : slices) {
                        if (slice.isEmpty() == false && slice.hasAllValues()[0] == false) {
                            function.selectedMayContainUnseenGroups(new SeenGroupIds.Empty());
                            break;
                        }
                    }
                    function.maybeEnsureCapacity(hash.numKeys() + 1);
                    for (int g = 0; g < slices.size(); g++) {
                        SliceRunReader.Slice slice = slices.get(g);
                        if (slice.isEmpty()) {
                            continue;
                        }
                        function.combinePartition(slice.states()[0], partition, appendOnly[g], ids[g], slice.keyCount());
                    }
                } finally {
                    for (SliceRunReader.Slice slice : slices) {
                        slice.release(breaker);
                    }
                }
                emit(driverContext, hash, aggregator, outputPages);
            }
            return rows(outputPages);
        } finally {
            Releasables.close(aggregator, decoder, hash, Releasables.wrap(runs));
            Releasables.close(outputPages);
            Releasables.close(pages);
            await(scope::close);
            await(store::shutdown);
        }
    }

    /**
     * Splits the hash and the aggregator's state and writes every non-empty partition to a slice run, which is opened
     * for reading with {@code decoder} as the state's reader.
     */
    private SliceRunReader spillGeneration(
        SpillScope scope,
        CircuitBreaker breaker,
        PartitionedBlockHash hash,
        GroupingAggregatorFunction function,
        GroupingAggregatorFunction decoder
    ) {
        GroupingAggregatorFunction.PartitionSplitter splitter = function.createPartitioningSplitter(breaker);
        PartitionedHashTable.PartitionedHashKeys keys;
        GroupingAggregatorFunction.PartitionedState state;
        try {
            keys = hash.splitPartition(breaker, splitter);
            try {
                state = splitter.finish();
            } catch (RuntimeException e) {
                keys.releaseAll(breaker);
                throw e;
            }
        } finally {
            splitter.release(breaker);
        }
        try {
            var options = new FramedRunWriter.Options(
                FramedRunLayout.KIND_SLICE,
                between(1024, 256 * 1024),
                between(1, 3),
                FramedRunMaxima.DEFAULT
            );
            FramedRunWriter runWriter = await(
                l -> FramedRunWriter.create(scope, "generation", breaker, EsExecutors.DIRECT_EXECUTOR_SERVICE, options, l)
            );
            SliceRunWriter writer = new SliceRunWriter(runWriter, 1);
            for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                final int partition = p;
                final int count = keys.keysInPartition(partition);
                if (count == 0) {
                    continue;
                }
                List<CheckedConsumer<StreamOutput, IOException>> states = List.of(out -> state.writePartition(partition, count, out));
                this.<Long>await(
                    l -> writer.append(
                        partition,
                        count,
                        new boolean[] { state.hasAllValues(partition) },
                        out -> hash.writePartitionKeys(keys, partition, out),
                        states,
                        null,
                        l
                    )
                );
            }
            SliceRunWriter.SliceRun run = await(writer::finish);
            for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                assertThat(run.hasSlice(p), equalTo(keys.keysInPartition(p) > 0));
            }
            FramedRunReader reader = await(
                l -> FramedRunReader.open(run.run().name(), run.run().object(), breaker, FramedRunMaxima.DEFAULT, between(1, 4096), l)
            );
            return new SliceRunReader(reader, hash, List.of(decoder), breaker);
        } finally {
            keys.releaseAll(breaker);
            state.releaseAll(breaker);
        }
    }

    private static void emit(DriverContext driverContext, PartitionedBlockHash hash, GroupingAggregator aggregator, List<Page> into) {
        try (IntVector selected = hash.nonEmpty(); var ctx = new GroupingAggregatorEvaluationContext(driverContext)) {
            if (selected.getPositionCount() == 0) {
                return;
            }
            try (GroupingAggregatorFunction.PreparedForEvaluation prepared = aggregator.prepareForEvaluate(selected, ctx)) {
                Block[] blocks = null;
                Block[] keys = hash.getKeys(selected);
                try {
                    blocks = new Block[keys.length + aggregator.evaluateBlockCount()];
                    System.arraycopy(keys, 0, blocks, 0, keys.length);
                    keys = null;
                    prepared.evaluate(blocks, blocks.length - aggregator.evaluateBlockCount(), selected);
                    into.add(new Page(blocks));
                    blocks = null;
                } finally {
                    if (keys != null) {
                        Releasables.close(keys);
                    }
                    if (blocks != null) {
                        Releasables.close(blocks);
                    }
                }
            }
        }
    }

    private <T> T await(Consumer<ActionListener<T>> operation) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        operation.accept(future);
        return future.actionGet(TimeValue.timeValueSeconds(30));
    }

    private static final GroupingAggregatorFunction.AddInput NO_INPUT = new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {}

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {}

        @Override
        public void add(int positionOffset, IntVector groupIds) {}

        @Override
        public void close() {}
    };

    static LongBlock longKeyBlock(BlockFactory blockFactory, int positionCount) {
        boolean singleValue = randomBoolean();
        try (var builder = blockFactory.newLongBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int valueCount = singleValue ? 1 : randomIntBetween(0, 3);
                if (valueCount == 0) {
                    builder.appendNull();
                } else if (valueCount == 1) {
                    builder.appendLong(between(1, 100));
                } else {
                    builder.beginPositionEntry();
                    for (int v = 0; v < valueCount; v++) {
                        builder.appendLong(between(1, 100));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static IntBlock intKeyBlock(BlockFactory blockFactory, int positionCount) {
        boolean singleValue = randomBoolean();
        try (var builder = blockFactory.newIntBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int valueCount = singleValue ? 1 : randomIntBetween(0, 3);
                if (valueCount == 0) {
                    builder.appendNull();
                } else if (valueCount == 1) {
                    builder.appendInt(between(1, 100));
                } else {
                    builder.beginPositionEntry();
                    for (int v = 0; v < valueCount; v++) {
                        builder.appendInt(between(1, 100));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private Function<DriverContext, BlockHash> newBlockHash() {
        if (randomBoolean()) {
            return dc -> new LongIntBlockHash(
                List.of(new BlockHash.GroupSpec(longKeyChannel, ElementType.LONG), new BlockHash.GroupSpec(intKeyChannel, ElementType.INT)),
                dc.blockFactory(),
                1024,
                false
            );
        } else {
            return dc -> BlockHash.buildPackedValuesBlockHash(
                List.of(new BlockHash.GroupSpec(longKeyChannel, ElementType.LONG), new BlockHash.GroupSpec(intKeyChannel, ElementType.INT)),
                dc.blockFactory(),
                1024
            );
        }
    }

    private void runTimeSeriesPartitioningTest(AggregatorFunctionSupplier supplier) {
        DriverContext driverContext = driverContext();
        try (var agg = supplier.groupingAggregator(driverContext, channels(AggregatorMode.SINGLE))) {
            assumeTrue("aggregator [" + agg + "] doesn't support partitioning", agg.supportPartitioning());
        }
        var pages = pagesWithTimeSeriesKeys(driverContext.blockFactory());
        try {
            List<TimeSeriesAggResult> singlePassResult = runTimeSeriesAggregation(driverContext(), supplier, pages, Integer.MAX_VALUE);
            assertThat(singlePassResult, not(empty()));
            List<TimeSeriesAggResult> partitioningResult = runTimeSeriesAggregation(driverContext(), supplier, pages, between(128, 1024));
            assertThat(partitioningResult, equalTo(singlePassResult));
        } finally {
            Releasables.close(pages);
        }
    }

    private List<Page> pagesWithTimeSeriesKeys(BlockFactory blockFactory) {
        List<Page> pages = CannedSourceOperator.collectPages(simpleInput(blockFactory, between(3000, 10_000)));
        int rowsPerTsid = between(1, 2);
        int bucketsPerTsid = between(1, 4);
        int row = 0;
        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            if (tsidChannel == -1) {
                tsidChannel = page.getBlockCount();
                timestampChannel = tsidChannel + 1;
            } else if (tsidChannel != page.getBlockCount()) {
                throw new AssertionError("expected [" + tsidChannel + "] blocks; got [" + page.getBlockCount());
            }
            Block[] blocks = new Block[page.getBlockCount() + 2];
            for (int b = 0; b < page.getBlockCount(); b++) {
                blocks[b] = page.getBlock(b);
            }
            int positionCount = page.getPositionCount();
            try (
                var tsids = blockFactory.newBytesRefVectorBuilder(positionCount);
                var timestamps = blockFactory.newLongVectorFixedBuilder(positionCount)
            ) {
                for (int p = 0; p < positionCount; p++, row++) {
                    int tsid = row / rowsPerTsid;
                    tsids.appendBytesRef(new BytesRef("tsid-" + tsid + (char) ('!' + tsid % 90)));
                    timestamps.appendLong(p, (row % bucketsPerTsid) * 60_000L);
                }
                blocks[tsidChannel] = tsids.build().asBlock();
                blocks[timestampChannel] = timestamps.build().asBlock();
            }
            pages.set(i, new Page(blocks));
        }
        return pages;
    }

    record TimeSeriesAggResult(String tsid, long timestamp, Object value) implements Comparable<TimeSeriesAggResult> {
        private static final Comparator<TimeSeriesAggResult> COMPARATOR = Comparator.comparing(TimeSeriesAggResult::tsid)
            .thenComparingLong(TimeSeriesAggResult::timestamp);

        @Override
        public int compareTo(TimeSeriesAggResult other) {
            return COMPARATOR.compare(this, other);
        }
    }

    private List<TimeSeriesAggResult> runTimeSeriesAggregation(
        DriverContext driverContext,
        AggregatorFunctionSupplier supplier,
        List<Page> inputPages,
        int partitionKeysThreshold
    ) {
        final TestThreadPool threadPool = newWorkerThreadPool();
        final List<Page> outputPages = new ArrayList<>();
        try {
            var factory = new TimeSeriesAggregationOperator.Factory(
                Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown(),
                false,
                List.of(
                    new BlockHash.GroupSpec(tsidChannel, ElementType.BYTES_REF),
                    new BlockHash.GroupSpec(timestampChannel, ElementType.LONG)
                ),
                AggregatorMode.SINGLE,
                List.of(supplier.groupingAggregatorFactory(AggregatorMode.SINGLE, channels(AggregatorMode.SINGLE))),
                randomIntBetween(128, 4096),
                randomIntBetween(128, 4096),
                TimeSeriesAggregationOperator.DEFAULT_PARTIAL_EMIT_KEYS_THRESHOLD,
                parallelConfig(threadPool, partitionKeysThreshold)
            );
            try (
                var source = new CannedSourceOperator(CannedSourceOperator.deepCopyOf(driverContext.blockFactory(), inputPages).iterator());
                Driver driver = TestDriverFactory.create(
                    driverContext,
                    source,
                    List.of(factory.get(driverContext)),
                    new TestResultPageSinkOperator(outputPages::add)
                )
            ) {
                new TestDriverRunner().run(driver);
                boolean promoted = driver.profile()
                    .operators()
                    .stream()
                    .anyMatch(status -> status.operator().equals(ParallelHashAggregationOperator.class.getSimpleName()));
                assertThat("promoted to the parallel operator", promoted, equalTo(partitionKeysThreshold < Integer.MAX_VALUE));
            }
            List<TimeSeriesAggResult> rows = new ArrayList<>();
            BytesRef scratch = new BytesRef();
            for (Page page : outputPages) {
                assertThat(page.getBlockCount(), equalTo(3));
                BytesRefBlock tsids = page.getBlock(0);
                LongBlock timestamps = page.getBlock(1);
                Block valueBlock = page.getBlock(2);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    rows.add(
                        new TimeSeriesAggResult(
                            tsids.getBytesRef(p, scratch).utf8ToString(),
                            timestamps.getLong(p),
                            BlockUtils.toJavaObject(valueBlock, p)
                        )
                    );
                }
            }
            return rows.stream().sorted().toList();
        } finally {
            Releasables.close(outputPages);
            terminate(threadPool);
        }
    }

    private TestThreadPool newWorkerThreadPool() {
        return new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                WORKER_EXECUTOR,
                between(1, 8),
                between(1, 1024),
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    private static HashAggregationOperator.ParallelConfig parallelConfig(TestThreadPool threadPool, int partitionKeysThreshold) {
        return new HashAggregationOperator.ParallelConfig(
            threadPool.executor(WORKER_EXECUTOR),
            between(1, 8),
            between(1, 10),
            partitionKeysThreshold
        );
    }
}
