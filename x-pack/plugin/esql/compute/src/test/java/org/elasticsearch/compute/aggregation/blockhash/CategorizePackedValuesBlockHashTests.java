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
import org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.compute.test.OperatorTestCase.runDriver;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CategorizePackedValuesBlockHashTests extends BlockHashTestCase {

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

    public void testCategorize_withDriver() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        DriverContext driverContext = new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));
        boolean withNull = randomBoolean();
        boolean withMultivalues = randomBoolean();

        List<BlockHash.GroupSpec> groupSpecs = List.of(
            new BlockHash.GroupSpec(0, ElementType.BYTES_REF, true),
            new BlockHash.GroupSpec(1, ElementType.INT, false)
        );

        LocalSourceOperator.BlockSupplier input1 = () -> {
            try (
                BytesRefBlock.Builder messagesBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(10);
                IntBlock.Builder idsBuilder = driverContext.blockFactory().newIntBlockBuilder(10)
            ) {
                if (withMultivalues) {
                    messagesBuilder.beginPositionEntry();
                }
                messagesBuilder.appendBytesRef(new BytesRef("connected to 1.1.1"));
                messagesBuilder.appendBytesRef(new BytesRef("connected to 1.1.2"));
                if (withMultivalues) {
                    messagesBuilder.endPositionEntry();
                }
                idsBuilder.appendInt(7);
                if (withMultivalues == false) {
                    idsBuilder.appendInt(7);
                }

                messagesBuilder.appendBytesRef(new BytesRef("connected to 1.1.3"));
                messagesBuilder.appendBytesRef(new BytesRef("connection error"));
                messagesBuilder.appendBytesRef(new BytesRef("connection error"));
                messagesBuilder.appendBytesRef(new BytesRef("connected to 1.1.4"));
                idsBuilder.appendInt(42);
                idsBuilder.appendInt(7);
                idsBuilder.appendInt(42);
                idsBuilder.appendInt(7);

                if (withNull) {
                    messagesBuilder.appendNull();
                    idsBuilder.appendInt(43);
                }
                return new Block[] { messagesBuilder.build(), idsBuilder.build() };
            }
        };
        LocalSourceOperator.BlockSupplier input2 = () -> {
            try (
                BytesRefBlock.Builder messagesBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(10);
                IntBlock.Builder idsBuilder = driverContext.blockFactory().newIntBlockBuilder(10)
            ) {
                messagesBuilder.appendBytesRef(new BytesRef("connected to 2.1.1"));
                messagesBuilder.appendBytesRef(new BytesRef("connected to 2.1.2"));
                messagesBuilder.appendBytesRef(new BytesRef("disconnected"));
                messagesBuilder.appendBytesRef(new BytesRef("connection error"));
                idsBuilder.appendInt(111);
                idsBuilder.appendInt(7);
                idsBuilder.appendInt(7);
                idsBuilder.appendInt(42);
                if (withNull) {
                    messagesBuilder.appendNull();
                    idsBuilder.appendNull();
                }
                return new Block[] { messagesBuilder.build(), idsBuilder.build() };
            }
        };

        List<Page> intermediateOutput = new ArrayList<>();

        Driver driver = new Driver(
            "test",
            driverContext,
            new LocalSourceOperator(input1),
            List.of(
                new HashAggregationOperator.HashAggregationOperatorFactory(
                    groupSpecs,
                    AggregatorMode.INITIAL,
                    List.of(new ValuesBytesRefAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.INITIAL, List.of(0))),
                    16 * 1024,
                    analysisRegistry
                ).get(driverContext)
            ),
            new PageConsumerOperator(intermediateOutput::add),
            () -> {}
        );
        runDriver(driver);

        driver = new Driver(
            "test",
            driverContext,
            new LocalSourceOperator(input2),
            List.of(
                new HashAggregationOperator.HashAggregationOperatorFactory(
                    groupSpecs,
                    AggregatorMode.INITIAL,
                    List.of(new ValuesBytesRefAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.INITIAL, List.of(0))),
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
            "test",
            driverContext,
            new CannedSourceOperator(intermediateOutput.iterator()),
            List.of(
                new HashAggregationOperator.HashAggregationOperatorFactory(
                    groupSpecs,
                    AggregatorMode.FINAL,
                    List.of(new ValuesBytesRefAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.FINAL, List.of(2))),
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
        BytesRefBlock outputMessages = finalOutput.get(0).getBlock(0);
        IntBlock outputIds = finalOutput.get(0).getBlock(1);
        BytesRefBlock outputValues = finalOutput.get(0).getBlock(2);
        assertThat(outputIds.getPositionCount(), equalTo(outputMessages.getPositionCount()));
        assertThat(outputValues.getPositionCount(), equalTo(outputMessages.getPositionCount()));
        Map<String, Map<Integer, Set<String>>> result = new HashMap<>();
        for (int i = 0; i < outputMessages.getPositionCount(); i++) {
            BytesRef messageBytesRef = ((BytesRef) BlockUtils.toJavaObject(outputMessages, i));
            String message = messageBytesRef == null ? null : messageBytesRef.utf8ToString();
            result.computeIfAbsent(message, key -> new HashMap<>());

            Integer id = (Integer) BlockUtils.toJavaObject(outputIds, i);
            result.get(message).computeIfAbsent(id, key -> new HashSet<>());

            Object values = BlockUtils.toJavaObject(outputValues, i);
            if (values == null) {
                result.get(message).get(id).add(null);
            } else {
                if ((values instanceof List) == false) {
                    values = List.of(values);
                }
                for (Object valueObject : (List<?>) values) {
                    BytesRef value = (BytesRef) valueObject;
                    result.get(message).get(id).add(value.utf8ToString());
                }
            }
        }
        Releasables.close(() -> Iterators.map(finalOutput.iterator(), (Page p) -> p::releaseBlocks));

        Map<String, Map<Integer, Set<String>>> expectedResult = Map.of(
            ".*?connected.+?to.*?",
            Map.of(
                7,
                Set.of("connected to 1.1.1", "connected to 1.1.2", "connected to 1.1.4", "connected to 2.1.2"),
                42,
                Set.of("connected to 1.1.3"),
                111,
                Set.of("connected to 2.1.1")
            ),
            ".*?connection.+?error.*?",
            Map.of(7, Set.of("connection error"), 42, Set.of("connection error")),
            ".*?disconnected.*?",
            Map.of(7, Set.of("disconnected"))
        );
        if (withNull) {
            expectedResult = new HashMap<>(expectedResult);
            expectedResult.put(null, new HashMap<>());
            expectedResult.get(null).put(null, new HashSet<>());
            expectedResult.get(null).get(null).add(null);
            expectedResult.get(null).put(43, new HashSet<>());
            expectedResult.get(null).get(43).add(null);
        }
        assertThat(result, equalTo(expectedResult));
    }
}
