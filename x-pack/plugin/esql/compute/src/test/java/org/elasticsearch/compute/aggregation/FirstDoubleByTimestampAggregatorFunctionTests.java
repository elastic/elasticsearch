/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.FirstLongByTimestampGroupingAggregatorFunctionTests.ExpectedWork;
import org.elasticsearch.compute.aggregation.FirstLongByTimestampGroupingAggregatorFunctionTests.TimestampGen;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.ListRowsBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;
import java.util.stream.IntStream;

public class FirstDoubleByTimestampAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        TimestampGen tsgen = randomFrom(TimestampGen.values());
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.DOUBLE, ElementType.LONG),
            IntStream.range(0, size).mapToObj(l -> List.of(randomDouble(), tsgen.gen())).toList()
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new FirstDoubleByTimestampAggregatorFunctionSupplier();
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "first_double_by_timestamp";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        ExpectedWork work = new ExpectedWork(true);
        for (Page page : input) {
            DoubleBlock values = page.getBlock(0);
            LongBlock timestamps = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
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
            }
        }
        work.check(BlockUtils.toJavaObject(result, 0));
    }
}
