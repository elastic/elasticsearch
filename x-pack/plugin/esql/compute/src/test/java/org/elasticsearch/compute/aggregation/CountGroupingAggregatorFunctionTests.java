/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.carrotsearch.hppc.IntLongHashMap;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class CountGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return CountAggregatorFunction.supplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        if (randomBoolean()) {
            return new TupleLongLongBlockSourceOperator(
                blockFactory,
                LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomLong()))
            );
        }
        return new LongDoubleTupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomDouble()))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        long count = input.stream().flatMapToInt(p -> allValueOffsets(p, group)).count();
        assertThat(((LongBlock) result).getLong(position), equalTo(count));
    }

    @Override
    protected void assertOutputFromNullOnly(Block b, int position) {
        assertThat(b.isNull(position), equalTo(false));
        assertThat(b.getValueCount(position), equalTo(1));
        assertThat(((LongBlock) b).getLong(b.getFirstValueIndex(position)), equalTo(0L));
    }

    @Override
    protected void assertOutputFromAllFiltered(Block b) {
        assertThat(b.elementType(), equalTo(ElementType.LONG));
        LongVector v = (LongVector) b.asVector();
        for (int p = 0; p < v.getPositionCount(); p++) {
            assertThat(v.getLong(p), equalTo(0L));
        }
    }

    public void testStates() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        final boolean bigValues = randomBoolean();
        try (var aggregator = new CountGroupingAggregatorFunction(List.of(1, 2), driverContext)) {
            int numPages = between(1, 20);
            int maxId = 0;
            IntLongHashMap expected = new IntLongHashMap();
            for (int i = 0; i < numPages; i++) {
                int positions = between(1, 1000);
                try (
                    var idBuilder = blockFactory.newIntVectorFixedBuilder(positions);
                    var valuesBuilder = blockFactory.newLongVectorFixedBuilder(positions);
                ) {
                    for (int p = 0; p < positions; p++) {
                        final int groupId = randomIntBetween(0, 10_000);
                        maxId = Math.max(groupId, maxId);
                        idBuilder.appendInt(p, groupId);
                        final long value;
                        if (bigValues) {
                            value = randomIntBetween(Integer.MAX_VALUE / 4, Integer.MAX_VALUE);
                        } else {
                            value = randomIntBetween(0, Integer.MAX_VALUE / 4);
                        }
                        expected.addTo(groupId, value);
                        valuesBuilder.appendLong(value);
                    }
                    try (
                        IntVector groupIds = idBuilder.build();
                        LongVector values = valuesBuilder.build();
                        var seen = blockFactory.newConstantBooleanVector(true, positions)
                    ) {
                        Page page = new Page(groupIds.asBlock(), values.asBlock(), seen.asBlock());
                        try (var addInput = aggregator.prepareProcessIntermediateInputPage(null, page)) {
                            addInput.add(0, groupIds);
                        }
                    }
                }
            }
            try (IntVector selected = blockFactory.newIntRangeVector(0, maxId + 1)) {
                try (var prepared = aggregator.prepareEvaluateFinal(selected, null)) {
                    Block[] blocks = new Block[1];
                    prepared.evaluate(blocks, 0, selected);
                    LongVector output = ((LongBlock) blocks[0]).asVector();
                    for (int i = 0; i < output.getPositionCount(); i++) {
                        long v = output.getLong(i);
                        assertThat(v, equalTo(expected.getOrDefault(i, 0)));
                    }
                    Releasables.close(blocks);
                }
            }
        }
    }
}
