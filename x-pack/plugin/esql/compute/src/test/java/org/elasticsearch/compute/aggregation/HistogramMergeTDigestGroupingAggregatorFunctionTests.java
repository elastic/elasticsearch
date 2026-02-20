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
import org.elasticsearch.compute.test.operator.blocksource.LongTDigestHistogramBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class HistogramMergeTDigestGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

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
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new LongTDigestHistogramBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), BlockTestUtils.randomTDigest()))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        List<TDigestHolder> allHistograms = input.stream().flatMap(p -> allTDigests(p, group)).toList();

        TDigestHolder value = null;
        if (result.isNull(position) == false) {
            value = ((TDigestBlock) result).getTDigestHolder(position);
        }

        if (allHistograms.isEmpty()) {
            assertThat(value, equalTo(null));
        } else {
            assertThat(TDigestTestUtils.isMergedFrom(value, allHistograms), equalTo(true));
        }
    }

    protected static Stream<TDigestHolder> allTDigests(Page page, Long group) {
        TDigestBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToObj(b::getTDigestHolder);
    }
}
