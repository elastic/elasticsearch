/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.FirstLongByTimestampGroupingAggregatorFunctionTests.ExpectedWork;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.util.List;
import java.util.stream.IntStream;

public class FirstDoubleByTimestampGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        FirstLongByTimestampGroupingAggregatorFunctionTests.TimestampGen tsgen = randomFrom(
            FirstLongByTimestampGroupingAggregatorFunctionTests.TimestampGen.values()
        );
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.LONG, ElementType.DOUBLE, ElementType.LONG),
            IntStream.range(0, size).mapToObj(l -> List.of(randomLongBetween(0, 4), randomDouble(), tsgen.gen())).toList()
        );
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new FirstDoubleByTimestampAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "first_double_by_timestamp";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        ExpectedWork work = new ExpectedWork(true);
        for (Page page : input) {
            matchingGroups(page, group).forEach(p -> {
                DoubleBlock values = page.getBlock(1);
                LongBlock timestamps = page.getBlock(2);
                int tsStart = timestamps.getFirstValueIndex(p);
                int tsEnd = tsStart + timestamps.getValueCount(p);
                for (int tsOffset = tsStart; tsOffset < tsEnd; tsOffset++) {
                    long timestamp = timestamps.getLong(tsOffset);
                    int vStart = values.getFirstValueIndex(p);
                    int vEnd = vStart + values.getValueCount(p);
                    for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
                        double value = values.getDouble(vOffset);
                        work.add(timestamp, value);
                    }
                }
            });
        }
        work.check(BlockUtils.toJavaObject(result, position));
    }
}
