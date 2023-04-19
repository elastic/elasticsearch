/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.LongBytesRefTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class CountDistinctBytesRefGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.COUNT_DISTINCT_BYTESREFS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count_distinct of bytesrefs";
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        int max = between(1, Math.min(1, Integer.MAX_VALUE / size));
        return new LongBytesRefTupleBlockSourceOperator(
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), new BytesRef(String.valueOf(between(-max, max)))))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        final int groupIndex = 0;
        final int valueIndex = 1;

        long expected = input.stream().flatMap(b -> IntStream.range(0, b.getPositionCount()).filter(p -> {
            LongBlock groupBlock = b.getBlock(groupIndex);
            Block valuesBlock = b.getBlock(valueIndex);
            return false == groupBlock.isNull(p) && false == valuesBlock.isNull(p) && groupBlock.getLong(p) == group;
        }).mapToObj(p -> ((BytesRefBlock) b.getBlock(valueIndex)).getBytesRef(p, new BytesRef()))).distinct().count();

        long count = ((LongBlock) result).getLong(position);
        if (expected == 0) {
            assertThat(count, equalTo(expected));
        } else {
            // HLL is an approximation algorithm and precision depends on the number of values computed and the precision_threshold param
            // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html
            // For a number of values close to 10k and precision_threshold=1000, precision should be less than 10%
            double precision = (double) count / (double) expected;
            assertThat(precision, closeTo(1.0, .1));
        }
    }
}
