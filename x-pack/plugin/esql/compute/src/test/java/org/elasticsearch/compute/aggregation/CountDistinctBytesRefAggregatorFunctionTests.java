/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.BytesRefBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

public class CountDistinctBytesRefAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(int size) {
        int max = between(1, Math.min(Integer.MAX_VALUE, Integer.MAX_VALUE / size));
        return new BytesRefBlockSourceOperator(
            LongStream.range(0, size).mapToObj(l -> new BytesRef(String.valueOf(between(-max, max)))).toList()
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, int inputChannel) {
        return CountDistinctBytesRefAggregator.supplier(bigArrays, inputChannel, 40000);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count_distinct of bytes";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        long expected = input.stream().flatMap(b -> allBytesRefs(b)).distinct().count();

        long count = ((LongBlock) result).getLong(0);
        // HLL is an approximation algorithm and precision depends on the number of values computed and the precision_threshold param
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html
        // For a number of values close to 10k and precision_threshold=1000, precision should be less than 10%
        assertThat((double) count, closeTo(expected, expected * 0.1));
    }
}
