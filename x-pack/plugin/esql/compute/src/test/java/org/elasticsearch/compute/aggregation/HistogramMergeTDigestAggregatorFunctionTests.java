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
import org.elasticsearch.compute.test.TDigestTestUtils;
import org.elasticsearch.compute.test.operator.blocksource.SequenceTDigestBlockSourceOperator;

import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class HistogramMergeTDigestAggregatorFunctionTests extends AggregatorFunctionTestCase {

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new HistogramMergeTDigestAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "histogram_merge of tdigests";
    }

    @Override
    protected boolean supportsMultiValues() {
        // tdigest blocks don't support multivalues (yet)
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
    protected void assertSimpleOutput(List<Page> input, Block result) {
        List<TDigestHolder> inputValues = input.stream().flatMap(p -> allTDigests(p.getBlock(0))).toList();
        TDigestHolder value = ((TDigestBlock) result).getTDigestHolder(0);

        assertThat(TDigestTestUtils.isMergedFrom(value, inputValues), equalTo(true));
    }

    protected static Stream<TDigestHolder> allTDigests(Block input) {
        TDigestBlock b = (TDigestBlock) input;
        return allValueOffsets(b).mapToObj(b::getTDigestHolder);
    }
}
