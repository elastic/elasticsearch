/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.operator.OperatorTestCase.runDriver;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CategorizeBlockHashTests extends BlockHashTestCase {

    private AnalysisRegistry analysisRegistry;

    @Before
    private void initAnalysisRegistry() throws IOException {
        analysisRegistry = new AnalysisModule(
            TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build()
            ),
            List.of(new MachineLearning(Settings.EMPTY), new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();
    }

    public void testCategorizeRaw() {
        final Page page;
        boolean withNull = randomBoolean();
        final int positions = 7 + (withNull ? 1 : 0);
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positions)) {
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.1"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Disconnected"));
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.2"));
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.3"));
            if (withNull) {
                if (randomBoolean()) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(new BytesRef(""));
                }
            }
            page = new Page(builder.build());
        }

        try (BlockHash hash = new CategorizeBlockHash(blockFactory, 0, AggregatorMode.INITIAL, analysisRegistry)) {
            hash.add(page, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    assertEquals(groupIds.getPositionCount(), positions);

                    assertEquals(1, groupIds.getInt(0));
                    assertEquals(2, groupIds.getInt(1));
                    assertEquals(2, groupIds.getInt(2));
                    assertEquals(2, groupIds.getInt(3));
                    assertEquals(3, groupIds.getInt(4));
                    assertEquals(1, groupIds.getInt(5));
                    assertEquals(1, groupIds.getInt(6));
                    if (withNull) {
                        assertEquals(0, groupIds.getInt(7));
                    }
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    add(positionOffset, groupIds.asBlock());
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
        } finally {
            page.releaseBlocks();
        }

        // TODO: randomize and try multiple pages.
        // TODO: assert the state of the BlockHash after adding pages. Including the categorizer state.
        // TODO: also test the lookup method and other stuff.
    }

    public void testCategorizeIntermediate() {
        Page page1;
        boolean withNull = randomBoolean();
        int positions1 = 7 + (withNull ? 1 : 0);
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positions1)) {
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.1"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.2"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.3"));
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.4"));
            if (withNull) {
                if (randomBoolean()) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(new BytesRef(""));
                }
            }
            page1 = new Page(builder.build());
        }
        Page page2;
        int positions2 = 5;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positions2)) {
            builder.appendBytesRef(new BytesRef("Disconnected"));
            builder.appendBytesRef(new BytesRef("Connected to 10.2.0.1"));
            builder.appendBytesRef(new BytesRef("Disconnected"));
            builder.appendBytesRef(new BytesRef("Connected to 10.3.0.2"));
            builder.appendBytesRef(new BytesRef("System shutdown"));
            page2 = new Page(builder.build());
        }

        Page intermediatePage1, intermediatePage2;

        // Fill intermediatePages with the intermediate state from the raw hashes
        try (
            BlockHash rawHash1 = new CategorizeBlockHash(blockFactory, 0, AggregatorMode.INITIAL, analysisRegistry);
            BlockHash rawHash2 = new CategorizeBlockHash(blockFactory, 0, AggregatorMode.INITIAL, analysisRegistry);
        ) {
            rawHash1.add(page1, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    assertEquals(groupIds.getPositionCount(), positions1);
                    assertEquals(1, groupIds.getInt(0));
                    assertEquals(2, groupIds.getInt(1));
                    assertEquals(2, groupIds.getInt(2));
                    assertEquals(1, groupIds.getInt(3));
                    assertEquals(2, groupIds.getInt(4));
                    assertEquals(1, groupIds.getInt(5));
                    assertEquals(1, groupIds.getInt(6));
                    if (withNull) {
                        assertEquals(0, groupIds.getInt(7));
                    }
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    add(positionOffset, groupIds.asBlock());
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
            intermediatePage1 = new Page(rawHash1.getKeys()[0]);

            rawHash2.add(page2, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    assertEquals(groupIds.getPositionCount(), positions2);
                    assertEquals(1, groupIds.getInt(0));
                    assertEquals(2, groupIds.getInt(1));
                    assertEquals(1, groupIds.getInt(2));
                    assertEquals(2, groupIds.getInt(3));
                    assertEquals(3, groupIds.getInt(4));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    add(positionOffset, groupIds.asBlock());
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
            intermediatePage2 = new Page(rawHash2.getKeys()[0]);
        } finally {
            page1.releaseBlocks();
            page2.releaseBlocks();
        }

        try (BlockHash intermediateHash = new CategorizeBlockHash(blockFactory, 0, AggregatorMode.INTERMEDIATE, null)) {
            intermediateHash.add(intermediatePage1, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    Set<Integer> values = IntStream.range(0, groupIds.getPositionCount())
                        .map(groupIds::getInt)
                        .boxed()
                        .collect(Collectors.toSet());
                    if (withNull) {
                        assertEquals(Set.of(0, 1, 2), values);
                    } else {
                        assertEquals(Set.of(1, 2), values);
                    }
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    add(positionOffset, groupIds.asBlock());
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });

            intermediateHash.add(intermediatePage2, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    Set<Integer> values = IntStream.range(0, groupIds.getPositionCount())
                        .map(groupIds::getInt)
                        .boxed()
                        .collect(Collectors.toSet());
                    // The category IDs {0, 1, 2} should map to groups {0, 2, 3}, because
                    // 0 matches an existing category (Connected to ...), and the others are new.
                    assertEquals(Set.of(1, 3, 4), values);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    add(positionOffset, groupIds.asBlock());
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
        } finally {
            intermediatePage1.releaseBlocks();
            intermediatePage2.releaseBlocks();
        }
    }

    public void testCategorize_withDriver() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        DriverContext driverContext = new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));

        LocalSourceOperator.BlockSupplier input1 = () -> {
            try (
                BytesRefVector.Builder textsBuilder = driverContext.blockFactory().newBytesRefVectorBuilder(10);
                LongVector.Builder countsBuilder = driverContext.blockFactory().newLongVectorBuilder(10)
            ) {
                // Note that just using "a" or "aaa" doesn't work, because the ml_standard
                // tokenizer drops numbers, including hexadecimal ones.
                textsBuilder.appendBytesRef(new BytesRef("aaazz"));
                textsBuilder.appendBytesRef(new BytesRef("bbbzz"));
                textsBuilder.appendBytesRef(new BytesRef("words words words goodbye jan"));
                textsBuilder.appendBytesRef(new BytesRef("words words words goodbye nik"));
                textsBuilder.appendBytesRef(new BytesRef("words words words goodbye tom"));
                textsBuilder.appendBytesRef(new BytesRef("words words words hello jan"));
                textsBuilder.appendBytesRef(new BytesRef("ccczz"));
                textsBuilder.appendBytesRef(new BytesRef("dddzz"));
                countsBuilder.appendLong(1);
                countsBuilder.appendLong(2);
                countsBuilder.appendLong(800);
                countsBuilder.appendLong(80);
                countsBuilder.appendLong(8000);
                countsBuilder.appendLong(900);
                countsBuilder.appendLong(30);
                countsBuilder.appendLong(4);
                return new Block[] { textsBuilder.build().asBlock(), countsBuilder.build().asBlock() };
            }
        };
        LocalSourceOperator.BlockSupplier input2 = () -> {
            try (
                BytesRefVector.Builder textsBuilder = driverContext.blockFactory().newBytesRefVectorBuilder(10);
                LongVector.Builder countsBuilder = driverContext.blockFactory().newLongVectorBuilder(10)
            ) {
                textsBuilder.appendBytesRef(new BytesRef("words words words hello nik"));
                textsBuilder.appendBytesRef(new BytesRef("words words words hello nik"));
                textsBuilder.appendBytesRef(new BytesRef("ccczz"));
                textsBuilder.appendBytesRef(new BytesRef("words words words goodbye chris"));
                textsBuilder.appendBytesRef(new BytesRef("dddzz"));
                textsBuilder.appendBytesRef(new BytesRef("eeezz"));
                countsBuilder.appendLong(9);
                countsBuilder.appendLong(90);
                countsBuilder.appendLong(3);
                countsBuilder.appendLong(8);
                countsBuilder.appendLong(40);
                countsBuilder.appendLong(5);
                return new Block[] { textsBuilder.build().asBlock(), countsBuilder.build().asBlock() };
            }
        };

        List<Page> intermediateOutput = new ArrayList<>();

        Driver driver = new Driver(
            driverContext,
            new LocalSourceOperator(input1),
            List.of(
                new HashAggregationOperator.HashAggregationOperatorFactory(
                    List.of(makeGroupSpec()),
                    AggregatorMode.INITIAL,
                    List.of(
                        new SumLongAggregatorFunctionSupplier(List.of(1)).groupingAggregatorFactory(AggregatorMode.INITIAL),
                        new MaxLongAggregatorFunctionSupplier(List.of(1)).groupingAggregatorFactory(AggregatorMode.INITIAL)
                    ),
                    16 * 1024,
                    analysisRegistry
                ).get(driverContext)
            ),
            new PageConsumerOperator(intermediateOutput::add),
            () -> {}
        );
        runDriver(driver);

        driver = new Driver(
            driverContext,
            new LocalSourceOperator(input2),
            List.of(
                new HashAggregationOperator.HashAggregationOperatorFactory(
                    List.of(makeGroupSpec()),
                    AggregatorMode.INITIAL,
                    List.of(
                        new SumLongAggregatorFunctionSupplier(List.of(1)).groupingAggregatorFactory(AggregatorMode.INITIAL),
                        new MaxLongAggregatorFunctionSupplier(List.of(1)).groupingAggregatorFactory(AggregatorMode.INITIAL)
                    ),
                    16 * 1024,
                    analysisRegistry
                ).get(driverContext)
            ),
            new PageConsumerOperator(intermediateOutput::add),
            () -> {}
        );
        runDriver(driver);

        List<Page> finalOutput = new ArrayList<>();

        driver = new Driver(
            driverContext,
            new CannedSourceOperator(intermediateOutput.iterator()),
            List.of(
                new HashAggregationOperator.HashAggregationOperatorFactory(
                    List.of(makeGroupSpec()),
                    AggregatorMode.FINAL,
                    List.of(
                        new SumLongAggregatorFunctionSupplier(List.of(1, 2)).groupingAggregatorFactory(AggregatorMode.FINAL),
                        new MaxLongAggregatorFunctionSupplier(List.of(3, 4)).groupingAggregatorFactory(AggregatorMode.FINAL)
                    ),
                    16 * 1024,
                    analysisRegistry
                ).get(driverContext)
            ),
            new PageConsumerOperator(finalOutput::add),
            () -> {}
        );
        runDriver(driver);

        assertThat(finalOutput, hasSize(1));
        assertThat(finalOutput.get(0).getBlockCount(), equalTo(3));
        BytesRefBlock outputTexts = finalOutput.get(0).getBlock(0);
        LongBlock outputSums = finalOutput.get(0).getBlock(1);
        LongBlock outputMaxs = finalOutput.get(0).getBlock(2);
        assertThat(outputSums.getPositionCount(), equalTo(outputTexts.getPositionCount()));
        assertThat(outputMaxs.getPositionCount(), equalTo(outputTexts.getPositionCount()));
        Map<String, Long> sums = new HashMap<>();
        Map<String, Long> maxs = new HashMap<>();
        for (int i = 0; i < outputTexts.getPositionCount(); i++) {
            sums.put(outputTexts.getBytesRef(i, new BytesRef()).utf8ToString(), outputSums.getLong(i));
            maxs.put(outputTexts.getBytesRef(i, new BytesRef()).utf8ToString(), outputMaxs.getLong(i));
        }
        assertThat(
            sums,
            equalTo(
                Map.of(
                    ".*?aaazz.*?",
                    1L,
                    ".*?bbbzz.*?",
                    2L,
                    ".*?ccczz.*?",
                    33L,
                    ".*?dddzz.*?",
                    44L,
                    ".*?eeezz.*?",
                    5L,
                    ".*?words.+?words.+?words.+?goodbye.*?",
                    8888L,
                    ".*?words.+?words.+?words.+?hello.*?",
                    999L
                )
            )
        );
        assertThat(
            maxs,
            equalTo(
                Map.of(
                    ".*?aaazz.*?",
                    1L,
                    ".*?bbbzz.*?",
                    2L,
                    ".*?ccczz.*?",
                    30L,
                    ".*?dddzz.*?",
                    40L,
                    ".*?eeezz.*?",
                    5L,
                    ".*?words.+?words.+?words.+?goodbye.*?",
                    8000L,
                    ".*?words.+?words.+?words.+?hello.*?",
                    900L
                )
            )
        );
        Releasables.close(() -> Iterators.map(finalOutput.iterator(), (Page p) -> p::releaseBlocks));
    }

    private BlockHash.GroupSpec makeGroupSpec() {
        return new BlockHash.GroupSpec(0, ElementType.BYTES_REF, true);
    }
}
