/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.ForkingOperatorTestCase;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.NullInsertingSourceOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class GroupingAggregatorFunctionTestCase extends ForkingOperatorTestCase {
    protected abstract GroupingAggregatorFunction.Factory aggregatorFunction();

    protected abstract String expectedDescriptionOfAggregator();

    protected abstract void assertSimpleGroup(List<Page> input, Block result, int position, long group);

    @FunctionalInterface
    interface GroupValueOffsetConsumer {
        void consume(LongBlock groups, int groupOffset, Block values, int valueOffset);
    }

    protected static void forEachGroupAndValue(List<Page> input, GroupValueOffsetConsumer consumer) {
        for (Page in : input) {
            int groupOffset = 0;
            int valueOffset = 0;
            for (int p = 0; p < in.getPositionCount(); p++) {
                Block groups = in.getBlock(0);
                Block values = in.getBlock(1);
                for (int groupValue = 0; groupValue < groups.getValueCount(p); groupValue++) {
                    if (groups.isNull(groupOffset + groupValue)) {
                        continue;
                    }
                    for (int valueValue = 0; valueValue < values.getValueCount(p); valueValue++) {
                        if (values.isNull(valueOffset + valueValue)) {
                            continue;
                        }
                        consumer.consume(in.getBlock(0), groupOffset + groupValue, in.getBlock(1), valueOffset + valueValue);
                    }
                }
                groupOffset += groups.getValueCount(p);
                valueOffset += values.getValueCount(p);
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simpleWithMode(BigArrays bigArrays, AggregatorMode mode) {
        return new HashAggregationOperator.HashAggregationOperatorFactory(
            0,
            List.of(new GroupingAggregator.GroupingAggregatorFactory(bigArrays, aggregatorFunction(), mode, 1)),
            () -> BlockHash.newLongHash(bigArrays)
        );
    }

    @Override
    protected final String expectedDescriptionOfSimple() {
        return "HashAggregationOperator(mode = <not-needed>, aggs = " + expectedDescriptionOfAggregator() + ")";
    }

    @Override
    protected final String expectedToStringOfSimple() {
        String type = getClass().getSimpleName().replace("Tests", "");
        return "HashAggregationOperator[groupByChannel=0, aggregators=[GroupingAggregator[aggregatorFunction="
            + type
            + "[channel=1], mode=SINGLE]]]";
    }

    @Override
    protected final void assertSimpleOutput(List<Page> input, List<Page> results) {
        SortedSet<Long> seenGroups = new TreeSet<>();
        forEachGroupAndValue(input, (groups, groupOffset, values, valueOffset) -> { seenGroups.add(groups.getLong(groupOffset)); });

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(2));
        assertThat(results.get(0).getPositionCount(), equalTo(seenGroups.size()));

        LongBlock groups = results.get(0).getBlock(0);
        Block result = results.get(0).getBlock(1);
        for (int i = 0; i < seenGroups.size(); i++) {
            long group = groups.getLong(i);
            assertSimpleGroup(input, result, i, group);
        }
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        return ByteSizeValue.ofBytes(between(1, 32));
    }

    public final void testIgnoresNulls() {
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(end));

        try (
            Driver d = new Driver(
                new NullInsertingSourceOperator(new CannedSourceOperator(input.iterator())),
                List.of(simple(nonBreakingBigArrays().withCircuitBreaking()).get()),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(input, results);
    }
}
