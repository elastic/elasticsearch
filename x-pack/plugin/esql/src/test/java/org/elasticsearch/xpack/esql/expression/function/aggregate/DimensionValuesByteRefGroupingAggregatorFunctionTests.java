/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.DimensionValuesByteRefGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DimensionValuesByteRefGroupingAggregatorFunctionTests extends ComputeTestCase {

    protected final DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory);
    }

    public void testSimple() {
        DriverContext driverContext = driverContext();
        int numPages = between(1, 10);
        List<Page> pages = new ArrayList<>(numPages);
        Map<Integer, List<BytesRef>> expectedValues = new HashMap<>();
        for (int i = 0; i < numPages; i++) {
            int positions = between(1, 100);
            try (
                var valuesBuilder = blockFactory().newBytesRefBlockBuilder(positions);
                var groups = blockFactory().newIntVectorFixedBuilder(positions)
            ) {
                for (int p = 0; p < positions; p++) {
                    int numValues = randomBoolean() ? 1 : between(0, 3);
                    List<BytesRef> values = new ArrayList<>();
                    for (int v = 0; v < numValues; v++) {
                        values.add(new BytesRef(randomAlphanumericOfLength(20)));
                    }
                    if (values.isEmpty()) {
                        valuesBuilder.appendNull();
                    } else if (values.size() == 1) {
                        valuesBuilder.appendBytesRef(values.getFirst());
                    } else {
                        valuesBuilder.beginPositionEntry();
                        for (BytesRef value : values) {
                            valuesBuilder.appendBytesRef(value);
                        }
                        valuesBuilder.endPositionEntry();
                    }
                    int group = between(0, 1000);
                    groups.appendInt(group);
                    expectedValues.putIfAbsent(group, values);
                }
                pages.add(new Page(valuesBuilder.build(), groups.build().asBlock()));
            }
        }
        var aggregatorFactory = new DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier().groupingAggregatorFactory(
            randomFrom(AggregatorMode.INITIAL, AggregatorMode.SINGLE),
            List.of(0)
        );
        final List<BlockHash.GroupSpec> groupSpecs;
        if (randomBoolean()) {
            // Use IntBlockHash; reserves 0 for null values
            groupSpecs = List.of(new BlockHash.GroupSpec(1, ElementType.INT));
        } else {
            // Use PackedValuesBlockHash; does not reserve 0 for null values
            groupSpecs = List.of(new BlockHash.GroupSpec(1, ElementType.INT), new BlockHash.GroupSpec(1, ElementType.INT));
        }
        HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
            List.of(aggregatorFactory),
            () -> BlockHash.build(groupSpecs, driverContext.blockFactory(), randomIntBetween(1, 1024), randomBoolean()),
            driverContext
        );
        List<Page> outputPages = new ArrayList<>();
        Driver driver = TestDriverFactory.create(
            driverContext,
            new CannedSourceOperator(pages.iterator()),
            List.of(hashAggregationOperator),
            new PageConsumerOperator(outputPages::add)
        );
        OperatorTestCase.runDriver(driver);

        Map<Integer, List<BytesRef>> actualValues = new HashMap<>();
        for (Page out : outputPages) {
            IntBlock groups = out.getBlock(0);
            BytesRefBlock valuesBlock = out.getBlock(groupSpecs.size());
            for (int p = 0; p < out.getPositionCount(); p++) {
                int group = groups.getInt(p);
                int valueCount = valuesBlock.getValueCount(p);
                if (valueCount == 0) {
                    actualValues.put(group, List.of());
                } else {
                    int firstValueIndex = valuesBlock.getFirstValueIndex(p);
                    List<BytesRef> values = new ArrayList<>(valueCount);
                    for (int v = 0; v < valueCount; v++) {
                        BytesRef dv = valuesBlock.getBytesRef(firstValueIndex + v, new BytesRef());
                        values.add(BytesRef.deepCopyOf(dv));
                    }
                    actualValues.put(group, values);
                }
            }
            out.close();
        }
        assertThat(actualValues, Matchers.equalTo(expectedValues));
    }
}
