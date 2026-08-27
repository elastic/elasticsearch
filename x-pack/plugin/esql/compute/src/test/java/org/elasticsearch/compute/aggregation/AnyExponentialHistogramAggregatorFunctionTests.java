/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.operator.blocksource.SequenceExponentialHistogramBlockSourceOperator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;

public class AnyExponentialHistogramAggregatorFunctionTests extends AggregatorFunctionTestCase {

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
        return new SequenceExponentialHistogramBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(i -> BlockTestUtils.randomExponentialHistogram())
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AnyExponentialHistogramAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "any_ExponentialHistogram";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        List<ExponentialHistogram> allInputs = input.stream().flatMap(p -> allExponentialHistograms(p.getBlock(0))).toList();
        if (result.isNull(0)) {
            assertThat(allInputs, empty());
            return;
        }
        ExponentialHistogramBlock resultBlock = (ExponentialHistogramBlock) result;
        ExponentialHistogram actual = resultBlock.getExponentialHistogram(
            resultBlock.getFirstValueIndex(0),
            new ExponentialHistogramScratch()
        );
        assertTrue(allInputs.stream().anyMatch(h -> ExponentialHistogram.equals(actual, h)));
    }

    static Stream<ExponentialHistogram> allExponentialHistograms(Block block) {
        ExponentialHistogramBlock b = (ExponentialHistogramBlock) block;
        return allValueOffsets(b).mapToObj(i -> b.getExponentialHistogram(i, new ExponentialHistogramScratch()));
    }
}
