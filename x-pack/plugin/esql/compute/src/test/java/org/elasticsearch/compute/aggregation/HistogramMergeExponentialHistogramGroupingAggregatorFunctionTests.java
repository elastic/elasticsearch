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
import org.elasticsearch.compute.test.operator.blocksource.LongExponentialHistogramBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils;

import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class HistogramMergeExponentialHistogramGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

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
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new LongExponentialHistogramBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), BlockTestUtils.randomExponentialHistogram()))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        List<ExponentialHistogram> allHistograms = input.stream().flatMap(p -> allExponentialHistograms(p, group)).toList();
        ExponentialHistogram expected = null;
        if (allHistograms.isEmpty() == false) {
            ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(ExponentialHistogramCircuitBreaker.noop());
            allHistograms.forEach(merger::add);
            expected = merger.get();
        }

        ExponentialHistogram value = null;
        if (result.isNull(position) == false) {
            value = ((ExponentialHistogramBlock) result).getExponentialHistogram(position, new ExponentialHistogramScratch());
        }

        if (value != null && expected != null) {
            ExponentialHistogramUtils.HistogramPair lenientHistograms = ExponentialHistogramUtils.removeMergeNoise(value, expected);
            assertThat(lenientHistograms.first(), equalTo(lenientHistograms.second()));
        } else {
            assertThat(value, equalTo(expected));
        }
    }

    protected static Stream<ExponentialHistogram> allExponentialHistograms(Page page, Long group) {
        ExponentialHistogramBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToObj(i -> b.getExponentialHistogram(i, new ExponentialHistogramScratch()));
    }
}
