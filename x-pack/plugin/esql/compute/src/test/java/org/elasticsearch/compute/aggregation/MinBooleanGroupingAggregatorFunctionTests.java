/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class MinBooleanGroupingAggregatorFunctionTests extends PartitionedGroupingAggregatorFunctionTestCase {
    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MinBooleanAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min of booleans";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.LONG, ElementType.BOOLEAN),
            IntStream.range(0, size).mapToObj(l -> List.<Object>of(randomLongBetween(0, 4), randomBoolean())).toList()
        );
    }

    @Override
    public void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        List<Boolean> values = new ArrayList<>();
        for (Page page : input) {
            LongBlock groups = page.getBlock(0);
            BooleanBlock bools = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                boolean inGroup = group == null
                    ? groups.isNull(p)
                    : groups.isNull(p) == false && groups.getLong(groups.getFirstValueIndex(p)) == group;
                if (inGroup == false || bools.isNull(p)) {
                    continue;
                }
                int start = bools.getFirstValueIndex(p);
                for (int i = start; i < start + bools.getValueCount(p); i++) {
                    values.add(bools.getBoolean(i));
                }
            }
        }
        if (values.isEmpty()) {
            assertThat(result.isNull(position), equalTo(true));
            return;
        }
        assertThat(result.isNull(position), equalTo(false));
        assertThat(((BooleanBlock) result).getBoolean(position), equalTo(values.stream().allMatch(b -> b)));
    }
}
