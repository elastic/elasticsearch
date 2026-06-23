/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;

import java.util.List;

public class AllFirstTDigestByIntAggregatorFunctionTests extends AggregatorFunctionTestCase {

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
        var vb = blockFactory.newTDigestBlockBuilder(size);
        var kb = blockFactory.newIntBlockBuilder(size);
        for (int i = 0; i < size; i++) {
            vb.appendTDigest(BlockTestUtils.randomTDigest());
            kb.appendInt(randomInt());
        }
        return new CannedSourceOperator(List.of(new Page(vb.build(), kb.build())).iterator());
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AllFirstTDigestByIntAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "all_first_TDigest_by_int";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        TDigestHolder expected = null;
        int minKey = Integer.MAX_VALUE;
        for (Page page : input) {
            TDigestBlock valueBlock = (TDigestBlock) page.getBlock(0);
            IntBlock keyBlock = (IntBlock) page.getBlock(1);
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                if (valueBlock.isNull(pos) || keyBlock.isNull(pos)) {
                    continue;
                }
                int key = keyBlock.getInt(keyBlock.getFirstValueIndex(pos));
                if (expected == null || key < minKey) {
                    minKey = key;
                    expected = valueBlock.getTDigestHolder(valueBlock.getFirstValueIndex(pos), new TDigestHolder());
                }
            }
        }
        if (expected == null) {
            assertTrue(result.isNull(0));
        } else {
            assertFalse(result.isNull(0));
            TDigestBlock tdBlock = (TDigestBlock) result;
            TDigestHolder actual = tdBlock.getTDigestHolder(tdBlock.getFirstValueIndex(0), new TDigestHolder());
            assertEquals(expected, actual);
        }
    }
}
