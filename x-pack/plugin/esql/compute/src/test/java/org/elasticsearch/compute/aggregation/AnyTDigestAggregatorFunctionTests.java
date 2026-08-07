/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.operator.blocksource.SequenceTDigestBlockSourceOperator;

import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;

public class AnyTDigestAggregatorFunctionTests extends AggregatorFunctionTestCase {

    @Override
    protected boolean skipInsertingNullRows() {
        return true;
    }

    @Override
    protected boolean supportsMultiValues() {
        return false;
    }

    @Override
    protected int maximumTestRowCount() {
        return 10_000;
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceTDigestBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(i -> BlockTestUtils.randomTDigest())
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AnyTDigestAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "any_TDigest";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        List<TDigestHolder> allInputs = input.stream().flatMap(p -> allTDigests(p.getBlock(0))).toList();
        if (result.isNull(0)) {
            assertThat(allInputs, empty());
            return;
        }
        TDigestBlock tdBlock = (TDigestBlock) result;
        TDigestHolder actual = tdBlock.getTDigestHolder(tdBlock.getFirstValueIndex(0), new TDigestHolder());
        assertTrue(allInputs.stream().anyMatch(actual::equals));
    }

    static Stream<TDigestHolder> allTDigests(Block block) {
        TDigestBlock b = (TDigestBlock) block;
        return allValueOffsets(b).mapToObj(i -> b.getTDigestHolder(i, new TDigestHolder()));
    }
}
