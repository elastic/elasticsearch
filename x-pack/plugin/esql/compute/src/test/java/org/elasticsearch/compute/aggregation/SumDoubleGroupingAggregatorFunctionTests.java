/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SumDoubleGroupingAggregatorFunctionTests extends PartitionedGroupingAggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new LongDoubleTupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, end).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomDouble()))
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SumDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of doubles";
    }

    @Override
    List<Page> pagesWithKeys(BlockFactory blockFactory) {
        List<Page> pages = super.pagesWithKeys(blockFactory);
        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            DoubleBlock original = page.getBlock(1);
            Block[] blocks = new Block[page.getBlockCount()];
            for (int b = 0; b < blocks.length; b++) {
                blocks[b] = page.getBlock(b);
            }
            boolean injectNulls = randomBoolean();
            boolean injectInfs = randomBoolean();
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(page.getPositionCount())) {
                for (int p = 0; p < page.getPositionCount(); p++) {
                    int n = randomInt(100);
                    if (injectNulls && n < 2) {
                        builder.appendNull();
                    } else if (injectInfs && 2 <= n && n < 4) {
                        builder.appendDouble(randomFrom(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN));
                    } else {
                        builder.copyFrom(original, p);
                    }
                }
                blocks[1] = builder.build();
            }
            original.close();
            pages.set(i, new Page(blocks));
        }
        return pages;
    }

    @Override
    protected void assertAggregatedResults(List<AggResult> singlePassResult, List<AggResult> partitioningResult) {
        assertThat(partitioningResult.size(), equalTo(singlePassResult.size()));
        for (int i = 0; i < singlePassResult.size(); i++) {
            AggResult singlePass = singlePassResult.get(i);
            AggResult partitioned = partitioningResult.get(i);
            assertThat(partitioned.key(), equalTo(singlePass.key()));
            if (singlePass.value() == null) {
                assertThat(partitioned.value(), nullValue());
            } else {
                double value = (Double) singlePass.value();
                if (Double.isFinite(value)) {
                    assertThat((Double) partitioned.value(), closeTo(value, Math.max(1e-10, Math.abs(value) * 1e-12)));
                } else {
                    assertThat(partitioned.value(), equalTo(singlePass.value()));
                }
            }
        }
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        CompensatedSum sum = new CompensatedSum();
        input.stream().flatMapToDouble(p -> allDoubles(p, group)).forEach(sum::add);
        // Won't precisely match in distributed case but will be close
        assertThat(((DoubleBlock) result).getDouble(position), closeTo(sum.value(), 0.01));
    }
}
