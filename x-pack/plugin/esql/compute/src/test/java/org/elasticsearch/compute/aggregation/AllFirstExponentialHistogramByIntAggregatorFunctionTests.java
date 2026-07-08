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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.util.List;

public class AllFirstExponentialHistogramByIntAggregatorFunctionTests extends AggregatorFunctionTestCase {

    @Override
    protected boolean supportsMultiValues() {
        return false;
    }

    @Override
    protected int maximumTestRowCount() {
        return 10_000;
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        var vb = blockFactory.newExponentialHistogramBlockBuilder(size);
        var kb = blockFactory.newIntBlockBuilder(size);
        for (int i = 0; i < size; i++) {
            vb.append(BlockTestUtils.randomExponentialHistogram());
            kb.appendInt(randomInt());
        }
        return new CannedSourceOperator(List.of(new Page(vb.build(), kb.build())).iterator());
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AllFirstExponentialHistogramByIntAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "all_first_ExponentialHistogram_by_int";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        ExponentialHistogram expected = null;
        int minKey = Integer.MAX_VALUE;
        for (Page page : input) {
            ExponentialHistogramBlock valueBlock = (ExponentialHistogramBlock) page.getBlock(0);
            IntBlock keyBlock = (IntBlock) page.getBlock(1);
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                if (valueBlock.isNull(pos) || keyBlock.isNull(pos)) {
                    continue;
                }
                int key = keyBlock.getInt(keyBlock.getFirstValueIndex(pos));
                if (expected == null || key < minKey) {
                    minKey = key;
                    expected = valueBlock.getExponentialHistogram(valueBlock.getFirstValueIndex(pos), new ExponentialHistogramScratch());
                }
            }
        }
        if (expected == null) {
            assertTrue(result.isNull(0));
        } else {
            assertFalse(result.isNull(0));
            ExponentialHistogramBlock resultBlock = (ExponentialHistogramBlock) result;
            ExponentialHistogram actual = resultBlock.getExponentialHistogram(
                resultBlock.getFirstValueIndex(0),
                new ExponentialHistogramScratch()
            );
            assertTrue(ExponentialHistogram.equals(expected, actual));
        }
    }
}
