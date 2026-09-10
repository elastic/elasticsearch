/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ParallelHashAggregationOperatorTests extends ComputeTestCase {
    private static final String ESQL_TEST_EXECUTOR = "esql_test_executor";
    private static final String SMALL_TEST_EXECUTOR = "esql_small_test_executor";

    private TestThreadPool threadPool;
    private final List<DriverContext> driverContexts = Collections.synchronizedList(new ArrayList<>());

    protected final DriverContext driverContext() {
        return driverContext(blockFactory());
    }

    protected final DriverContext crankyDriverContext() {
        return driverContext(crankyBlockFactory());
    }

    Executor randomWorkerExecutor() {
        return threadPool.executor(randomFrom(ESQL_TEST_EXECUTOR, SMALL_TEST_EXECUTOR));
    }

    protected DriverContext driverContext(BlockFactory blockFactory) {
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        driverContexts.add(driverContext);
        return driverContext;
    }

    public void testSmall() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(1, 256)
        );
        DriverContext driverContext = driverContext();
        runTest(between(100, 1000), driverContext.blockFactory(), driverContext, config);
    }

    public void testLarge() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(256, 4 * 1024)
        );
        DriverContext driverContext = driverContext();
        runTest(between(10 * 1024, 100 * 1000), driverContext.blockFactory(), driverContext(), config);
    }

    public void testRejectionButOkay() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            threadPool.executor(SMALL_TEST_EXECUTOR),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(256, 4 * 1024)
        );
        DriverContext driverContext = driverContext();
        runTest(between(10 * 1024, 100 * 1000), driverContext.blockFactory(), driverContext(), config);
    }

    public void testCranky() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            randomIntBetween(256, 1024)
        );
        try {
            runTest(between(1000, 100 * 1000), driverContext().blockFactory(), crankyDriverContext(), config);
        } catch (CircuitBreakingException ignored) {

        }
    }

    public void testStatus() {
        HashAggregationOperator.ParallelConfig config = new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 32),
            randomIntBetween(1, 1024),
            1024
        );
        DriverContext driverContext = driverContext();
        var status = runTest(4096, driverContext.blockFactory(), driverContext, config);
        assertThat(status.completedOperators(), hasSize(3));
        OperatorStatus operatorStatus = status.completedOperators().get(1);
        assertThat(operatorStatus.operator(), equalTo("ParallelHashAggregationOperator"));
    }

    record Key(long longValue, int intValue) {}

    record Row(Key key, long[] counts) {}

    DriverStatus runTest(
        int numValues,
        BlockFactory sourceBlockFactory,
        DriverContext driverContext,
        HashAggregationOperator.ParallelConfig parallelConfig
    ) {
        int countAggregations = between(0, 4);
        List<Row> inputRows = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
            long[] counts = new long[countAggregations];
            for (int v = 0; v < countAggregations; v++) {
                counts[v] = randomIntBetween(0, Integer.MAX_VALUE);
            }
            Key key = new Key(randomLongBetween(0, numValues * 2L), randomIntBetween(0, numValues * 2));
            inputRows.add(new Row(key, counts));
        }
        List<GroupingAggregator.Factory> aggregatorFactories = new ArrayList<>(countAggregations);
        for (int a = 0; a < countAggregations; a++) {
            final int valueChannel = 2 + 2 * a;
            aggregatorFactories.add(
                CountAggregatorFunction.supplier().groupingAggregatorFactory(AggregatorMode.FINAL, List.of(valueChannel, valueChannel + 1))
            );
        }
        Map<Key, long[]> expected = expected(inputRows, countAggregations);
        List<Page> inputPages = inputPages(sourceBlockFactory, inputRows, countAggregations);
        var groupSpecs = List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.INT));
        List<Page> outputPages = new ArrayList<>();
        final DriverStatus status;
        try (SourceOperator sourceOperator = new CannedSourceOperator(inputPages.iterator())) {
            final Function<DriverContext, BlockHash> blockHashSupplier;
            if (randomBoolean()) {
                blockHashSupplier = dc -> BlockHash.build(groupSpecs, dc.blockFactory(), between(128, 1024), false);
            } else {
                blockHashSupplier = dc -> BlockHash.buildPackedValuesBlockHash(groupSpecs, dc.blockFactory(), between(128, 1024));
            }
            HashAggregationOperator hashOperator = new HashAggregationOperator(
                AggregatorMode.FINAL,
                aggregatorFactories,
                blockHashSupplier,
                randomIntBetween(1, 1024),
                randomDouble(),
                randomIntBetween(128, 4096),
                null,
                null,
                driverContext,
                parallelConfig
            );
            try (
                Driver d = TestDriverFactory.create(
                    driverContext,
                    sourceOperator,
                    List.of(hashOperator),
                    new TestResultPageSinkOperator(outputPages::add),
                    TimeValue.timeValueNanos(randomIntBetween(1, 1000_000_000)),
                    () -> {}
                )
            ) {
                new TestDriverRunner().run(d);
                status = d.status();
            }
            Map<Key, long[]> actual = new HashMap<>();
            for (Page page : outputPages) {
                assertThat(page.getBlockCount(), equalTo(2 + countAggregations));
                LongBlock longBlock = page.getBlock(0);
                IntBlock intBlock = page.getBlock(1);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    long[] counts = new long[countAggregations];
                    for (int a = 0; a < countAggregations; a++) {
                        counts[a] = ((LongBlock) page.getBlock(2 + a)).getLong(i);
                    }
                    assertNull(actual.put(new Key(longBlock.getLong(i), intBlock.getInt(i)), counts));
                }
            }
            assertThat(actual.keySet(), equalTo(expected.keySet()));
            for (Key k : actual.keySet()) {
                assertArrayEquals(k.toString(), actual.get(k), expected.get(k));
            }
        } finally {
            Releasables.close(outputPages);
        }
        return status;
    }

    static Map<Key, long[]> expected(List<Row> rows, int countAggregations) {
        Map<Key, long[]> expected = new HashMap<>();
        for (Row row : rows) {
            expected.compute(row.key, (k, v) -> {
                if (v == null) {
                    return row.counts;
                } else {
                    long[] sum = new long[countAggregations];
                    for (int i = 0; i < countAggregations; i++) {
                        sum[i] = v[i] + row.counts[i];
                    }
                    return sum;
                }
            });
        }
        return expected;
    }

    static List<Page> inputPages(BlockFactory blockFactory, List<Row> rows, int countAggregations) {
        List<Page> pages = new ArrayList<>();
        boolean success = false;
        try {
            for (int start = 0; start < rows.size();) {
                int end = Math.min(rows.size(), start + between(1, 1024));
                pages.add(inputPage(blockFactory, rows.subList(start, end), countAggregations));
                start = end;
            }
            success = true;
            return pages;
        } finally {
            if (success == false) {
                Releasables.close(pages);
            }
        }
    }

    static Page inputPage(BlockFactory blockFactory, List<Row> rows, int countAggregations) {
        Block[] blocks = new Block[2 + countAggregations * 2];
        List<LongBlock.Builder> countBuilders = new ArrayList<>(countAggregations);
        boolean success = false;
        try (
            LongBlock.Builder longKeys = blockFactory.newLongBlockBuilder(rows.size());
            IntBlock.Builder intKeys = blockFactory.newIntBlockBuilder(rows.size())
        ) {
            for (int a = 0; a < countAggregations; a++) {
                countBuilders.add(blockFactory.newLongBlockBuilder(rows.size()));
            }
            for (Row row : rows) {
                longKeys.appendLong(row.key.longValue);
                intKeys.appendInt(row.key.intValue);
                for (int a = 0; a < countAggregations; a++) {
                    countBuilders.get(a).appendLong(row.counts[a]);
                }
            }
            blocks[0] = longKeys.build();
            blocks[1] = intKeys.build();
            for (int a = 0; a < countAggregations; a++) {
                blocks[2 + 2 * a] = countBuilders.get(a).build();
                blocks[2 + 2 * a + 1] = blockFactory.newConstantBooleanBlockWith(true, rows.size());
            }
            success = true;
            return new Page(blocks);
        } finally {
            Releasables.close(countBuilders);
            if (success == false) {
                Releasables.close(blocks);
            }
        }
    }

    // ---- single-key tests (IntBlockHash / LongBlockHash / BytesRefBlockHash) ----

    public void testSingleIntKey() {
        assumeTrue("requires partitioning support", PartitionedBlockHash.supportPartitioning());
        runSingleKeyTest(SingleKeyType.INT, between(100, 2000), blockFactory(), driverContext(), normalParallelConfig());
    }

    public void testSingleLongKey() {
        assumeTrue("requires partitioning support", PartitionedBlockHash.supportPartitioning());
        runSingleKeyTest(SingleKeyType.LONG, between(100, 2000), blockFactory(), driverContext(), normalParallelConfig());
    }

    public void testSingleBytesRefKey() {
        assumeTrue("requires partitioning support", PartitionedBlockHash.supportPartitioning());
        runSingleKeyTest(SingleKeyType.BYTES_REF, between(100, 2000), blockFactory(), driverContext(), normalParallelConfig());
    }

    public void testCrankySingleIntKey() {
        assumeTrue("requires partitioning support", PartitionedBlockHash.supportPartitioning());
        try {
            runSingleKeyTest(SingleKeyType.INT, between(100, 2000), crankyBlockFactory(), crankyDriverContext(), normalParallelConfig());
        } catch (CircuitBreakingException ignored) {}
    }

    public void testCrankySingleLongKey() {
        assumeTrue("requires partitioning support", PartitionedBlockHash.supportPartitioning());
        try {
            runSingleKeyTest(SingleKeyType.LONG, between(100, 2000), crankyBlockFactory(), crankyDriverContext(), normalParallelConfig());
        } catch (CircuitBreakingException ignored) {}
    }

    public void testCrankySingleBytesRefKey() {
        assumeTrue("requires partitioning support", PartitionedBlockHash.supportPartitioning());
        try {
            runSingleKeyTest(
                SingleKeyType.BYTES_REF,
                between(100, 2000),
                crankyBlockFactory(),
                crankyDriverContext(),
                normalParallelConfig()
            );
        } catch (CircuitBreakingException ignored) {}
    }

    private HashAggregationOperator.ParallelConfig normalParallelConfig() {
        return new HashAggregationOperator.ParallelConfig(
            randomWorkerExecutor(),
            randomIntBetween(1, 8),
            randomIntBetween(1, 1024),
            randomIntBetween(64, 512)
        );
    }

    enum SingleKeyType {
        INT,
        LONG,
        BYTES_REF
    }

    /**
     * Runs a COUNT aggregation grouped by a single key column with partitioning enabled.
     * Input pages: key (channel 0), count value (channel 1), seen (channel 2).
     * The block hash reads channel 0; the COUNT FINAL aggregator reads channels 1 and 2.
     */
    void runSingleKeyTest(
        SingleKeyType keyType,
        int numRows,
        BlockFactory sourceBlockFactory,
        DriverContext driverContext,
        HashAggregationOperator.ParallelConfig parallelConfig
    ) {
        int keyCardinality = between(2, 20);
        List<Object> inputKeys = new ArrayList<>(numRows);
        List<Long> inputValues = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            inputKeys.add(rarely() ? null : randomSingleKey(keyType, keyCardinality));
            inputValues.add(randomLongBetween(1, 100));
        }

        Map<Object, Long> expected = new HashMap<>();
        for (int i = 0; i < inputKeys.size(); i++) {
            expected.merge(inputKeys.get(i), inputValues.get(i), Long::sum);
        }

        List<Page> inputPages = buildSingleKeyPages(sourceBlockFactory, keyType, inputKeys, inputValues);
        List<Page> outputPages = new ArrayList<>();
        HashAggregationOperator hashOperator = null;
        try {
            hashOperator = new HashAggregationOperator(
                AggregatorMode.FINAL,
                List.of(CountAggregatorFunction.supplier().groupingAggregatorFactory(AggregatorMode.FINAL, List.of(1, 2))),
                dc -> buildSingleKeyHash(keyType, dc.blockFactory()),
                randomIntBetween(1, 1024),
                randomDouble(),
                randomIntBetween(128, 4096),
                null,
                null,
                driverContext,
                parallelConfig
            );
            try (
                SourceOperator source = new CannedSourceOperator(inputPages.iterator());
                Driver d = TestDriverFactory.create(
                    driverContext,
                    source,
                    List.of(hashOperator),
                    new TestResultPageSinkOperator(outputPages::add)
                )
            ) {
                hashOperator = null;
                new TestDriverRunner().run(d);
            }
            Map<Object, Long> actual = extractSingleKeyResults(keyType, outputPages);
            assertThat(actual.keySet(), equalTo(expected.keySet()));
            for (Object key : actual.keySet()) {
                assertThat("count for key=" + key, actual.get(key), equalTo(expected.get(key)));
            }
        } finally {
            Releasables.close(hashOperator, Releasables.wrap(outputPages));
        }
    }

    private static Object randomSingleKey(SingleKeyType keyType, int cardinality) {
        return switch (keyType) {
            case INT -> randomIntBetween(0, cardinality - 1);
            case LONG -> (long) randomIntBetween(0, cardinality - 1);
            case BYTES_REF -> new BytesRef(String.valueOf(randomIntBetween(0, cardinality - 1)));
        };
    }

    private static BlockHash buildSingleKeyHash(SingleKeyType keyType, BlockFactory blockFactory) {
        ElementType elementType = switch (keyType) {
            case INT -> ElementType.INT;
            case LONG -> ElementType.LONG;
            case BYTES_REF -> ElementType.BYTES_REF;
        };
        return BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, between(128, 1024), false);
    }

    private static List<Page> buildSingleKeyPages(BlockFactory blockFactory, SingleKeyType keyType, List<Object> keys, List<Long> values) {
        List<Page> pages = new ArrayList<>();
        boolean success = false;
        try {
            for (int start = 0; start < keys.size();) {
                int end = Math.min(keys.size(), start + between(1, 512));
                pages.add(buildSingleKeyPage(blockFactory, keyType, keys.subList(start, end), values.subList(start, end)));
                start = end;
            }
            success = true;
            return pages;
        } finally {
            if (success == false) {
                Releasables.close(pages);
            }
        }
    }

    private static Page buildSingleKeyPage(BlockFactory blockFactory, SingleKeyType keyType, List<Object> keys, List<Long> values) {
        Block[] blocks = new Block[3];
        boolean success = false;
        try {
            blocks[0] = switch (keyType) {
                case INT -> {
                    try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(keys.size())) {
                        for (Object k : keys) {
                            if (k == null) b.appendNull();
                            else b.appendInt((int) k);
                        }
                        yield b.build();
                    }
                }
                case LONG -> {
                    try (LongBlock.Builder b = blockFactory.newLongBlockBuilder(keys.size())) {
                        for (Object k : keys) {
                            if (k == null) b.appendNull();
                            else b.appendLong((long) k);
                        }
                        yield b.build();
                    }
                }
                case BYTES_REF -> {
                    try (BytesRefBlock.Builder b = blockFactory.newBytesRefBlockBuilder(keys.size())) {
                        for (Object k : keys) {
                            if (k == null) b.appendNull();
                            else b.appendBytesRef((BytesRef) k);
                        }
                        yield b.build();
                    }
                }
            };
            try (LongBlock.Builder valBuilder = blockFactory.newLongBlockBuilder(values.size())) {
                for (Long v : values) {
                    valBuilder.appendLong(v);
                }
                blocks[1] = valBuilder.build();
            }
            blocks[2] = blockFactory.newConstantBooleanBlockWith(true, keys.size());
            success = true;
            return new Page(blocks);
        } finally {
            if (success == false) {
                Releasables.close(blocks);
            }
        }
    }

    private static Map<Object, Long> extractSingleKeyResults(SingleKeyType keyType, List<Page> pages) {
        Map<Object, Long> actual = new HashMap<>();
        for (Page page : pages) {
            assertThat(page.getBlockCount(), equalTo(2));
            LongBlock counts = page.getBlock(1);
            switch (keyType) {
                case INT -> {
                    IntBlock keys = page.getBlock(0);
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        assertNull(actual.put(keys.isNull(i) ? null : keys.getInt(i), counts.getLong(i)));
                    }
                }
                case LONG -> {
                    LongBlock keys = page.getBlock(0);
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        assertNull(actual.put(keys.isNull(i) ? null : keys.getLong(i), counts.getLong(i)));
                    }
                }
                case BYTES_REF -> {
                    BytesRefBlock keys = page.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        Object key = keys.isNull(i) ? null : BytesRef.deepCopyOf(keys.getBytesRef(i, scratch));
                        assertNull(actual.put(key, counts.getLong(i)));
                    }
                }
            }
        }
        return actual;
    }

    // ---- thread pool setup ----

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                ESQL_TEST_EXECUTOR,
                between(1, 32),
                randomIntBetween(1, 1024),
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            ),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                SMALL_TEST_EXECUTOR,
                between(1, 2),
                randomIntBetween(1, 4),
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @After
    public void shutdownThreadPool() {
        if (threadPool != null) {
            terminate(threadPool);
            threadPool = null;
        }
    }
}
