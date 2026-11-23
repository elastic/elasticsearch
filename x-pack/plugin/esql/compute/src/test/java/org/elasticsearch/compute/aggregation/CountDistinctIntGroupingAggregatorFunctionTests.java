/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.LongIntBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class CountDistinctIntGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new CountDistinctIntAggregatorFunctionSupplier(CountDistinctTestUtils.PRECISION);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count_distinct of ints";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new LongIntBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomGroupId(size), between(0, 10000)))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        CountDistinctTestUtils.assertCount(
            ((LongBlock) result).getLong(position),
            input.stream().flatMapToInt(p -> allInts(p, group)).distinct().mapToLong(CountDistinctTestUtils::hash)
        );
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
}
