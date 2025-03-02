/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.LongBytesRefTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class MaxBytesRefGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new LongBytesRefTupleBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), new BytesRef(randomAlphaOfLengthBetween(0, 100))))
        );
    }

    @Override
    protected DataType acceptedDataType() {
        return DataType.IP;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MaxBytesRefAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "max of bytes";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        Optional<BytesRef> max = input.stream().flatMap(p -> allBytesRefs(p, group)).max(Comparator.naturalOrder());
        if (max.isEmpty()) {
            assertThat(result.isNull(position), equalTo(true));
            return;
        }
        assertThat(result.isNull(position), equalTo(false));
        assertThat(BlockUtils.toJavaObject(result, position), equalTo(max.get()));
    }
}
