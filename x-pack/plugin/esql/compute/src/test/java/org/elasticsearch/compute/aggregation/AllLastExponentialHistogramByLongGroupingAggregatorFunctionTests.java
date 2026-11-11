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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.util.List;

public class AllLastExponentialHistogramByLongGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected boolean supportsMultiValues() {
        return false;
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        var gb = blockFactory.newLongBlockBuilder(size);
        var vb = blockFactory.newExponentialHistogramBlockBuilder(size);
        var kb = blockFactory.newLongBlockBuilder(size);
        for (int i = 0; i < size; i++) {
            gb.appendLong(randomLongBetween(0, 4));
            vb.append(BlockTestUtils.randomExponentialHistogram());
            kb.appendLong(randomLong());
        }
        return new CannedSourceOperator(List.of(new Page(gb.build(), vb.build(), kb.build())).iterator());
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AllLastExponentialHistogramByLongAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "all_last_ExponentialHistogram_by_long";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        ExponentialHistogram expected = null;
        long maxKey = Long.MIN_VALUE;
        for (Page page : input) {
            ExponentialHistogramBlock valueBlock = (ExponentialHistogramBlock) page.getBlock(1);
            LongBlock keyBlock = (LongBlock) page.getBlock(2);
            for (int pos : (Iterable<Integer>) () -> matchingGroups(page, group).iterator()) {
                if (valueBlock.isNull(pos) || keyBlock.isNull(pos)) {
                    continue;
                }
                long key = keyBlock.getLong(keyBlock.getFirstValueIndex(pos));
                if (expected == null || key > maxKey) {
                    maxKey = key;
                    expected = valueBlock.getExponentialHistogram(valueBlock.getFirstValueIndex(pos), new ExponentialHistogramScratch());
                }
            }
        }
        if (expected == null) {
            assertTrue(result.isNull(position));
        } else {
            assertFalse(result.isNull(position));
            ExponentialHistogramBlock resultBlock = (ExponentialHistogramBlock) result;
            ExponentialHistogram actual = resultBlock.getExponentialHistogram(
                resultBlock.getFirstValueIndex(position),
                new ExponentialHistogramScratch()
            );
            assertTrue(ExponentialHistogram.equals(expected, actual));
        }
    }
}
