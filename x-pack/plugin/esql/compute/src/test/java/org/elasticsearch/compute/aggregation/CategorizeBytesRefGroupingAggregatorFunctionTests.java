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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class CategorizeBytesRefGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    private static final int NUM_PREFIXES = 10;

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<String> prefixes = IntStream.range(0, NUM_PREFIXES)
            .mapToObj(i -> randomAlphaOfLength(5) + " " + randomAlphaOfLength(5) + " " + randomAlphaOfLength(5) + " ")
            .toList();

        return new LongBytesRefTupleBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size)
                .mapToObj(
                    i -> Tuple.tuple(
                        randomGroupId(size),
                        new BytesRef((prefixes.get(i % NUM_PREFIXES) + i).getBytes(StandardCharsets.UTF_8))
                    )
                )
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new CategorizeBytesRefAggregatorFunctionSupplier(inputChannels);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "categorize of bytes";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        int inputSize = (int) input.stream()
            .flatMap(p -> GroupingAggregatorFunctionTestCase.allBytesRefs(p, group))
            .filter(Objects::nonNull)
            .map(b -> b.utf8ToString().replaceAll("[0-9]", ""))
            .distinct()
            .count();
        Object resultValue = BlockUtils.toJavaObject(result, position);
        switch (inputSize) {
            case 0 -> assertThat(resultValue, nullValue());
            case 1 -> assertThat(resultValue, instanceOf(BytesRef.class));
            default -> {
                assertThat(resultValue, instanceOf(List.class));
                assertThat(((List<?>) resultValue).size(), equalTo(inputSize));
            }
        }
    }
}
