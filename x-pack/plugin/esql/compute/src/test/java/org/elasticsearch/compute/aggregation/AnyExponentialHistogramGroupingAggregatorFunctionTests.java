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

import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class AnyExponentialHistogramGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected boolean supportsMultiValues() {
        return false;
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new LongExponentialHistogramBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(i -> Tuple.tuple(randomLongBetween(0, 4), BlockTestUtils.randomExponentialHistogram()))
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
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        List<ExponentialHistogram> groupValues = input.stream().flatMap(p -> allExponentialHistograms(p, group)).toList();
        if (groupValues.isEmpty()) {
            assertTrue(result.isNull(position));
        } else {
            assertFalse(result.isNull(position));
            ExponentialHistogramBlock resultBlock = (ExponentialHistogramBlock) result;
            ExponentialHistogram actual = resultBlock.getExponentialHistogram(
                resultBlock.getFirstValueIndex(position),
                new ExponentialHistogramScratch()
            );
            assertTrue(groupValues.stream().anyMatch(h -> ExponentialHistogram.equals(actual, h)));
        }
    }

    static Stream<ExponentialHistogram> allExponentialHistograms(Page page, Long group) {
        ExponentialHistogramBlock b = (ExponentialHistogramBlock) page.getBlock(1);
        return allValueOffsets(page, group).mapToObj(i -> b.getExponentialHistogram(i, new ExponentialHistogramScratch()));
    }
}
