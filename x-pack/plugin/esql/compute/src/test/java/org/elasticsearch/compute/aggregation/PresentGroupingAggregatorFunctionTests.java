/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class PresentGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return PresentAggregatorFunction.supplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "present";
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
        boolean present = input.stream().flatMapToInt(p -> allValueOffsets(p, group)).findAny().isPresent();
        assertThat(((BooleanBlock) result).getBoolean(position), equalTo(present));
    }

    @Override
    protected void assertOutputFromNullOnly(Block b, int position) {
        assertThat(b.isNull(position), equalTo(false));
        assertThat(b.getValueCount(position), equalTo(1));
        assertThat(((BooleanBlock) b).getBoolean(b.getFirstValueIndex(position)), equalTo(false));
    }

    @Override
    protected void assertOutputFromAllFiltered(Block b) {
        assertThat(b.elementType(), equalTo(ElementType.BOOLEAN));
        BooleanVector v = (BooleanVector) b.asVector();
        for (int p = 0; p < v.getPositionCount(); p++) {
            assertThat(v.getBoolean(p), equalTo(false));
        }
    }
}
