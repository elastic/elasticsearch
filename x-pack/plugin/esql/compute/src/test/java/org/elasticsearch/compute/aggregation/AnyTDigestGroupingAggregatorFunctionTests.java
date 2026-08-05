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
import org.elasticsearch.compute.test.operator.blocksource.LongTDigestHistogramBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class AnyTDigestGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected boolean supportsMultiValues() {
        return false;
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new LongTDigestHistogramBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(i -> Tuple.tuple(randomLongBetween(0, 4), BlockTestUtils.randomTDigest()))
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
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        List<TDigestHolder> groupValues = input.stream().flatMap(p -> allTDigests(p, group)).toList();
        if (groupValues.isEmpty()) {
            assertTrue(result.isNull(position));
        } else {
            assertFalse(result.isNull(position));
            TDigestBlock tdBlock = (TDigestBlock) result;
            TDigestHolder actual = tdBlock.getTDigestHolder(tdBlock.getFirstValueIndex(position), new TDigestHolder());
            assertTrue(groupValues.stream().anyMatch(actual::equals));
        }
    }

    static Stream<TDigestHolder> allTDigests(Page page, Long group) {
        TDigestBlock b = (TDigestBlock) page.getBlock(1);
        return allValueOffsets(page, group).mapToObj(i -> b.getTDigestHolder(i, new TDigestHolder()));
    }
}
