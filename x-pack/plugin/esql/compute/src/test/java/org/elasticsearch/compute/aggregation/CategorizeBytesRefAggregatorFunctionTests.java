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
import org.elasticsearch.compute.operator.SequenceBytesRefBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class CategorizeBytesRefAggregatorFunctionTests extends AggregatorFunctionTestCase {

    private static final int NUM_PREFIXES = 10;

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<String> prefixes = IntStream.range(0, NUM_PREFIXES)
            .mapToObj(i -> randomAlphaOfLength(5) + " " + randomAlphaOfLength(5) + " " + randomAlphaOfLength(5) + " ")
            .toList();
        return new SequenceBytesRefBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(i -> new BytesRef((prefixes.get(i % NUM_PREFIXES) + i).getBytes(StandardCharsets.UTF_8)))
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
    public void assertSimpleOutput(List<Block> input, Block result) {
        long resultSize = ((List<?>) Objects.requireNonNull(BlockUtils.toJavaObject(result, 0))).size();
        long inputSize = input.stream().flatMap(AggregatorFunctionTestCase::allBytesRefs).count();
        assertThat(resultSize, equalTo(Math.min(inputSize, NUM_PREFIXES)));
    }
}
