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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.compute.operator.OperatorTestCase.runDriver;
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

        List<BlockHash.GroupSpec> groupSpecs = List.of(
            new BlockHash.GroupSpec(0, ElementType.BYTES_REF, true),
            new BlockHash.GroupSpec(1, ElementType.INT, false)
        );

        LocalSourceOperator.BlockSupplier input1 = () -> {
            try (
                BytesRefVector.Builder messagesBuilder = driverContext.blockFactory().newBytesRefVectorBuilder(10);
                IntVector.Builder idsBuilder = driverContext.blockFactory().newIntVectorBuilder(10)
            ) {
                messagesBuilder.appendBytesRef(new BytesRef("connected to 1.1.1"));
                messagesBuilder.appendBytesRef(new BytesRef("connected to 1.1.2"));
                messagesBuilder.appendBytesRef(new BytesRef("connected to 1.1.3"));
                messagesBuilder.appendBytesRef(new BytesRef("connection error"));
                messagesBuilder.appendBytesRef(new BytesRef("connection error"));
                messagesBuilder.appendBytesRef(new BytesRef("connected to 1.1.4"));
                idsBuilder.appendInt(7);
                idsBuilder.appendInt(7);
                idsBuilder.appendInt(42);
                idsBuilder.appendInt(7);
                idsBuilder.appendInt(42);
                idsBuilder.appendInt(7);
                return new Block[] { messagesBuilder.build().asBlock(), idsBuilder.build().asBlock() };
            }
        };
        LocalSourceOperator.BlockSupplier input2 = () -> {
            try (
                BytesRefVector.Builder messagesBuilder = driverContext.blockFactory().newBytesRefVectorBuilder(10);
                IntVector.Builder idsBuilder = driverContext.blockFactory().newIntVectorBuilder(10)
            ) {
                messagesBuilder.appendBytesRef(new BytesRef("connected to 2.1.1"));
                messagesBuilder.appendBytesRef(new BytesRef("connected to 2.1.2"));
                messagesBuilder.appendBytesRef(new BytesRef("disconnected"));
                messagesBuilder.appendBytesRef(new BytesRef("connection error"));
                idsBuilder.appendInt(111);
                idsBuilder.appendInt(7);
                idsBuilder.appendInt(7);
                idsBuilder.appendInt(42);
                return new Block[] { messagesBuilder.build().asBlock(), idsBuilder.build().asBlock() };
            }
        };

        List<Page> intermediateOutput = new ArrayList<>();

        Driver driver = new Driver(
            driverContext,
            new LocalSourceOperator(input1),
            List.of(
                new HashAggregationOperator.HashAggregationOperatorFactory(
                    groupSpecs,
                    AggregatorMode.INITIAL,
                    List.of(new ValuesBytesRefAggregatorFunctionSupplier(List.of(0)).groupingAggregatorFactory(AggregatorMode.INITIAL)),
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
                    groupSpecs,
                    AggregatorMode.INITIAL,
                    List.of(new ValuesBytesRefAggregatorFunctionSupplier(List.of(0)).groupingAggregatorFactory(AggregatorMode.INITIAL)),
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
                    groupSpecs,
                    AggregatorMode.FINAL,
                    List.of(new ValuesBytesRefAggregatorFunctionSupplier(List.of(2)).groupingAggregatorFactory(AggregatorMode.FINAL)),
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
        Map<String, Map<Integer, Set<String>>> values = new HashMap<>();
        for (int i = 0; i < outputMessages.getPositionCount(); i++) {
            String message = outputMessages.getBytesRef(i, new BytesRef()).utf8ToString();
            int id = outputIds.getInt(i);
            int valuesFromIndex = outputValues.getFirstValueIndex(i);
            int valuesToIndex = valuesFromIndex + outputValues.getValueCount(i);
            for (int valueIndex = valuesFromIndex; valueIndex < valuesToIndex; valueIndex++) {
                String value = outputValues.getBytesRef(valueIndex, new BytesRef()).utf8ToString();
                values.computeIfAbsent(message, key -> new HashMap<>()).computeIfAbsent(id, key -> new HashSet<>()).add(value);
            }
        }
        Releasables.close(() -> Iterators.map(finalOutput.iterator(), (Page p) -> p::releaseBlocks));

        assertThat(
            values,
            equalTo(
                Map.of(
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
                )
            )
        );
    }
}
