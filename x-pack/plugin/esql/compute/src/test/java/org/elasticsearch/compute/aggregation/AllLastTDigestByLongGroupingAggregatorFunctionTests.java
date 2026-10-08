/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;

import java.util.List;

public class AllLastTDigestByLongGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

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
        var vb = blockFactory.newTDigestBlockBuilder(size);
        var kb = blockFactory.newLongBlockBuilder(size);
        for (int i = 0; i < size; i++) {
            gb.appendLong(randomLongBetween(0, 4));
            vb.appendTDigest(BlockTestUtils.randomTDigest());
            kb.appendLong(randomLong());
        }
        return new CannedSourceOperator(List.of(new Page(gb.build(), vb.build(), kb.build())).iterator());
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AllLastTDigestByLongAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "all_last_TDigest_by_long";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        TDigestHolder expected = null;
        long maxKey = Long.MIN_VALUE;
        for (Page page : input) {
            TDigestBlock valueBlock = (TDigestBlock) page.getBlock(1);
            LongBlock keyBlock = (LongBlock) page.getBlock(2);
            for (int pos : (Iterable<Integer>) () -> matchingGroups(page, group).iterator()) {
                if (valueBlock.isNull(pos) || keyBlock.isNull(pos)) {
                    continue;
                }
                long key = keyBlock.getLong(keyBlock.getFirstValueIndex(pos));
                if (expected == null || key > maxKey) {
                    maxKey = key;
                    expected = valueBlock.getTDigestHolder(valueBlock.getFirstValueIndex(pos), new TDigestHolder());
                }
            }
        }
        if (expected == null) {
            assertTrue(result.isNull(position));
        } else {
            assertFalse(result.isNull(position));
            TDigestBlock tdBlock = (TDigestBlock) result;
            TDigestHolder actual = tdBlock.getTDigestHolder(tdBlock.getFirstValueIndex(position), new TDigestHolder());
            assertEquals(expected, actual);
        }
    }
}
