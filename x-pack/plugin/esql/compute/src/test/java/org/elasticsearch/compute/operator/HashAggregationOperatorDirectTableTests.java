/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests the switch of a partial {@link HashAggregationOperator} from a regular hash table to a direct-mapped one. The
 * results must be the same as without the switch, once the final aggregation merged the repeated keys.
 */
public class HashAggregationOperatorDirectTableTests extends ComputeTestCase {
    private static final String EXECUTOR = "esql_test_executor";

    private TestThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, EXECUTOR, between(1, 8), 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testLong() {
        runSwitching(List.of(ElementType.LONG), true);
    }

    public void testBytesRef() {
        runSwitching(List.of(ElementType.BYTES_REF), true);
    }

    public void testIntInt() {
        runSwitching(List.of(ElementType.INT, ElementType.INT), true);
    }

    public void testLongInt() {
        runSwitching(List.of(ElementType.LONG, ElementType.INT), true);
    }

    public void testIntLong() {
        runSwitching(List.of(ElementType.INT, ElementType.LONG), true);
    }

    public void testLongLong() {
        runSwitching(List.of(ElementType.LONG, ElementType.LONG), true);
    }

    public void testLongIntBytesRefDoesNotUseDirectTable() {
        // BYTES_REF in a multi key hash is not packed at a fixed width, so there is nothing to switch to.
        assertFalse(BlockHash.supportsDirect(groups(List.of(ElementType.LONG, ElementType.BYTES_REF))));
        runSwitching(List.of(ElementType.LONG, ElementType.BYTES_REF), false);
    }

    public void testIntIntLowCardinalityDoesNotSwitch() {
        runLowCardinality(List.of(ElementType.INT, ElementType.INT));
    }

    public void testLongLowCardinalityDoesNotSwitch() {
        runLowCardinality(List.of(ElementType.LONG));
    }

    /**
     * {@code SINGLE} emits final results, so a repeated key would be a wrong result. It must never use the direct table.
     */
    public void testSingleNeverSwitches() {
        List<ElementType> types = randomFrom(List.of(ElementType.LONG), List.of(ElementType.INT, ElementType.INT));
        List<Row> rows = rows(types, 60_000, 1_000_000);
        DriverContext driverContext = driverContext();
        List<Page> input = pages(driverContext.blockFactory(), types, rows);
        List<Page> output = new ArrayList<>();
        var operator = operator(AggregatorMode.SINGLE, types, driverContext, 1 << between(0, 10), 100, 0.0);
        try {
            run(driverContext, input, operator, output);
            assertFalse(operator.usesDirectTable());
            assertThat(results(types, output), equalTo(expected(rows)));
            // no key can be in two rows
            assertThat(output.stream().mapToInt(Page::getPositionCount).sum(), equalTo(expected(rows).size()));
        } finally {
            Releasables.close(output);
        }
    }

    private void runSwitching(List<ElementType> types, boolean expectSwitch) {
        // Almost every key is new, so the uniqueness check passes after the first window.
        List<Row> rows = rows(types, between(40_000, 100_000), 1_000_000);
        runInitialAndFinal(types, rows, 1 << between(0, 14), expectSwitch);
    }

    private void runLowCardinality(List<ElementType> types) {
        runInitialAndFinal(types, rows(types, 60_000, 10), 1 << between(0, 10), false);
    }

    private void runInitialAndFinal(List<ElementType> types, List<Row> rows, int slots, boolean expectSwitch) {
        DriverContext initialContext = driverContext();
        List<Page> input = pages(initialContext.blockFactory(), types, rows);
        List<Page> partial = new ArrayList<>();
        List<Page> output = new ArrayList<>();
        try {
            var initial = operator(AggregatorMode.INITIAL, types, initialContext, slots, between(10, 2000), 0.5);
            run(initialContext, input, initial, partial);
            assertThat(initial.usesDirectTable(), equalTo(expectSwitch));

            DriverContext finalContext = driverContext(initialContext.blockFactory().parent());
            var finalOperator = operator(AggregatorMode.FINAL, types, finalContext, slots, 0, 0);
            run(finalContext, partial, finalOperator, output);
            Map<List<Object>, Long> expected = expected(rows);
            Map<List<Object>, Long> actual = results(types, output);
            assertThat(actual, equalTo(expected));
            assertThat(actual.size(), greaterThan(0));
        } finally {
            Releasables.close(output);
            Releasables.close(partial);
        }
    }

    private DriverContext driverContext() {
        return driverContext(blockFactory());
    }

    private DriverContext driverContext(BlockFactory blockFactory) {
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private static List<BlockHash.GroupSpec> groups(List<ElementType> types) {
        List<BlockHash.GroupSpec> groups = new ArrayList<>();
        for (int c = 0; c < types.size(); c++) {
            groups.add(new BlockHash.GroupSpec(c, types.get(c)));
        }
        return groups;
    }

    private HashAggregationOperator operator(
        AggregatorMode mode,
        List<ElementType> types,
        DriverContext driverContext,
        int slots,
        int switchMinKeys,
        double switchUniqueness
    ) {
        List<BlockHash.GroupSpec> groups = groups(types);
        List<GroupingAggregator.Factory> aggregators = List.of(
            mode.isInputPartial()
                ? new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(mode, List.of(types.size(), types.size() + 1))
                : new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(mode, List.of(types.size()))
        );
        Function<DriverContext, BlockHash> regular = dc -> BlockHash.build(groups, dc.blockFactory(), 1024, false);
        var config = new HashAggregationOperator.ParallelConfig(threadPool.executor(EXECUTOR), between(1, 4), between(1, 8), 64 * 1024);
        if (BlockHash.supportsDirect(groups) == false) {
            return new HashAggregationOperator(mode, aggregators, regular, 100_000, 0.1, 1024, null, driverContext, config, true);
        }
        return new HashAggregationOperator(
            mode,
            aggregators,
            regular,
            dc -> BlockHash.buildDirect(groups, dc.blockFactory(), 1024, slots),
            new HashAggregationOperator.DirectTableConfig(slots, switchMinKeys, switchUniqueness),
            100_000,
            0.1,
            1024,
            null,
            driverContext,
            config,
            true
        );
    }

    private void run(DriverContext driverContext, List<Page> input, HashAggregationOperator operator, List<Page> output) {
        // Partitioned partial output can't be deep copied by TestResultPageSinkOperator, so only copy final results.
        SinkOperator sink = operator.aggregatorMode.isOutputPartial() ? new PageConsumerOperator(p -> {
            p.allowPassingToDifferentDriver();
            output.add(p);
        }) : new TestResultPageSinkOperator(output::add);
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(input.iterator()),
                List.of(operator),
                sink,
                TimeValue.timeValueNanos(randomIntBetween(1, 1000_000_000)),
                () -> {}
            )
        ) {
            new TestDriverRunner().run(d);
        }
    }

    /**
     * One input row. The keys are {@code null} for a missing value.
     */
    private record Row(List<Object> keys, int value) {}

    private List<Row> rows(List<ElementType> types, int count, int distinct) {
        List<Row> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int keyId = randomIntBetween(0, distinct - 1);
            List<Object> keys = new ArrayList<>();
            for (int c = 0; c < types.size(); c++) {
                // derive every column from the key id so that equal ids make equal keys, with some nulls
                int columnId = keyId * (c + 1) + c;
                if (columnId % 17 == 5) {
                    keys.add(null);
                    continue;
                }
                keys.add(switch (types.get(c)) {
                    case LONG -> columnId * 31L;
                    case INT -> columnId;
                    case BYTES_REF -> new BytesRef("key-" + columnId);
                    default -> throw new IllegalArgumentException("unsupported " + types.get(c));
                });
            }
            rows.add(new Row(keys, randomIntBetween(-1000, 1000)));
        }
        return rows;
    }

    private static Map<List<Object>, Long> expected(List<Row> rows) {
        Map<List<Object>, Long> expected = new HashMap<>();
        for (Row row : rows) {
            expected.merge(row.keys, (long) row.value, Long::sum);
        }
        return expected;
    }

    private List<Page> pages(BlockFactory blockFactory, List<ElementType> types, List<Row> rows) {
        List<Page> pages = new ArrayList<>();
        boolean success = false;
        try {
            for (int start = 0; start < rows.size();) {
                int end = Math.min(rows.size(), start + between(1, 4096));
                pages.add(page(blockFactory, types, rows.subList(start, end)));
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

    private Page page(BlockFactory blockFactory, List<ElementType> types, List<Row> rows) {
        Block[] blocks = new Block[types.size() + 1];
        boolean success = false;
        try {
            for (int c = 0; c < types.size(); c++) {
                try (Block.Builder builder = types.get(c).newBlockBuilder(rows.size(), blockFactory)) {
                    for (Row row : rows) {
                        Object key = row.keys.get(c);
                        if (key == null) {
                            builder.appendNull();
                            continue;
                        }
                        switch (types.get(c)) {
                            case LONG -> ((LongBlock.Builder) builder).appendLong((Long) key);
                            case INT -> ((IntBlock.Builder) builder).appendInt((Integer) key);
                            case BYTES_REF -> ((BytesRefBlock.Builder) builder).appendBytesRef((BytesRef) key);
                            default -> throw new IllegalArgumentException("unsupported");
                        }
                    }
                    blocks[c] = builder.build();
                }
            }
            try (IntBlock.Builder values = blockFactory.newIntBlockBuilder(rows.size())) {
                for (Row row : rows) {
                    values.appendInt(row.value);
                }
                blocks[types.size()] = values.build();
            }
            success = true;
            return new Page(blocks);
        } finally {
            if (success == false) {
                Releasables.close(blocks);
            }
        }
    }

    private static Map<List<Object>, Long> results(List<ElementType> types, List<Page> pages) {
        Map<List<Object>, Long> results = new HashMap<>();
        for (Page page : pages) {
            LongBlock sums = page.getBlock(types.size());
            for (int p = 0; p < page.getPositionCount(); p++) {
                List<Object> keys = new ArrayList<>();
                for (int c = 0; c < types.size(); c++) {
                    Block block = page.getBlock(c);
                    if (block.isNull(p)) {
                        keys.add(null);
                        continue;
                    }
                    int i = block.getFirstValueIndex(p);
                    assertThat(block.getValueCount(p), equalTo(1));
                    keys.add(switch (types.get(c)) {
                        case LONG -> ((LongBlock) block).getLong(i);
                        case INT -> ((IntBlock) block).getInt(i);
                        case BYTES_REF -> ((BytesRefBlock) block).getBytesRef(i, new BytesRef());
                        default -> throw new IllegalArgumentException("unsupported");
                    });
                }
                Object previous = results.put(keys, sums.getLong(sums.getFirstValueIndex(p)));
                assertNull("key " + keys + " in two rows of " + Arrays.toString(types.toArray()), previous);
            }
        }
        return results;
    }
}
