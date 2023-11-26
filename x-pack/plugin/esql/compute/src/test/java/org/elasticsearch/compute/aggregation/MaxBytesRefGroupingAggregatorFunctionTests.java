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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BytesRefBlockSourceOperator;
import org.elasticsearch.compute.operator.LongBytesRefTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class MaxBytesRefGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        int max = between(1, Math.min(Integer.MAX_VALUE, Integer.MAX_VALUE / size));
        return new LongBytesRefTupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomGroupId(size), new BytesRef(String.valueOf(between(1, max)))))
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, List<Integer> inputChannels) {
        return new MaxBytesRefAggregatorFunctionSupplier(bigArrays, inputChannels);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "max of bytes";
    }

    private static class Comparator implements java.util.Comparator<BytesRef> {
        @Override
        public int compare(BytesRef p1, BytesRef p2) {
            if (p2 == null) {
                // ignore nulls
                return 1;
            }
            return p1.compareTo(p2);
        }
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        Comparator c = new Comparator();
        Optional<BytesRef> max = input.stream().flatMap(b -> allBytesRefs(b, group)).max(c);
        if (max.isEmpty()) {
            assertThat(result.isNull(position), equalTo(true));
            return;
        }

        assertThat(result.isNull(position), equalTo(false));
        BytesRef spare = new BytesRef("");
        String s = ((BytesRefBlock) result).getBytesRef(position, spare).utf8ToString();
        assertEquals(s, max.get().utf8ToString());
    }
}
