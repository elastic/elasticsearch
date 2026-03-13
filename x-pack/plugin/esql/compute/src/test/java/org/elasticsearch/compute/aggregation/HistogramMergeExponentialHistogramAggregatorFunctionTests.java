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
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils;

import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class HistogramMergeExponentialHistogramAggregatorFunctionTests extends AggregatorFunctionTestCase {

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new HistogramMergeExponentialHistogramAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "histogram_merge of exponential_histograms";
    }

    @Override
    protected boolean supportsMultiValues() {
        // exponential histogram blocks don't support multivalues (yet)
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
    protected void assertSimpleOutput(List<Page> input, Block result) {
        ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(ExponentialHistogramCircuitBreaker.noop());
        input.stream().flatMap(p -> allExponentialHistograms(p.getBlock(0))).forEach(merger::add);
        ExponentialHistogram expected = merger.get();
        ExponentialHistogram value = ((ExponentialHistogramBlock) result).getExponentialHistogram(0, new ExponentialHistogramScratch());

        // the exact results depend on the merge order, so we have to be a bit lenient here
        ExponentialHistogramUtils.HistogramPair lenientHistograms = ExponentialHistogramUtils.removeMergeNoise(value, expected);

        assertThat(lenientHistograms.first(), equalTo(lenientHistograms.second()));
    }

    protected static Stream<ExponentialHistogram> allExponentialHistograms(Block input) {
        ExponentialHistogramBlock b = (ExponentialHistogramBlock) input;
        return allValueOffsets(b).mapToObj(i -> b.getExponentialHistogram(i, new ExponentialHistogramScratch()));
    }
}
