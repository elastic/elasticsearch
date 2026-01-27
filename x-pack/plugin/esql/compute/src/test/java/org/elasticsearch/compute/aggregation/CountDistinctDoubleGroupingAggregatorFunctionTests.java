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
import org.elasticsearch.compute.operator.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class CountDistinctDoubleGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new CountDistinctDoubleAggregatorFunctionSupplier(40000);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count_distinct of doubles";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new LongDoubleTupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomGroupId(size), randomDoubleBetween(0, 100, true)))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        long distinct = input.stream().flatMapToDouble(p -> allDoubles(p, group)).distinct().count();
        long count = ((LongBlock) result).getLong(position);
        // HLL is an approximation algorithm and precision depends on the number of values computed and the precision_threshold param
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html
        // For a number of values close to 10k and precision_threshold=1000, precision should be less than 10%
        assertThat((double) count, closeTo(distinct, distinct * 0.1));
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
