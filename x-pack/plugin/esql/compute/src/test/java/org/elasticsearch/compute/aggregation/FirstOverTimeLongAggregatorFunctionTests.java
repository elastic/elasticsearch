/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class FirstOverTimeLongAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new TupleLongLongBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLong(), randomLong()))
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new FirstOverTimeLongAggregatorFunctionSupplier();
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "first_over_time of longs";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        long v = (Long) BlockUtils.toJavaObject(result, 0);
        long expectedTimestamp = 0;
        Set<Long> expected = new HashSet<>();
        for (Page page : input) {
            LongBlock timestamps = page.<LongBlock>getBlock(0);
            LongBlock values = page.<LongBlock>getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                int tsStart = timestamps.getFirstValueIndex(p);
                int tsEnd = tsStart + timestamps.getValueCount(p);
                for (int tsOffset = tsStart; tsOffset < tsEnd; tsOffset++) {
                    long timestamp = timestamps.getLong(tsOffset);
                    int vStart = values.getFirstValueIndex(p);
                    int vEnd = vStart + values.getValueCount(p);
                    for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
                        long value = values.getLong(vOffset);
                        if (expected.isEmpty()) {
                            expectedTimestamp = timestamp;
                            expected.add(value);
                        } else if (timestamp < expectedTimestamp) {
                            expectedTimestamp = timestamp;
                            expected.clear();
                            expected.add(value);
                        } else if (timestamp == expectedTimestamp) {
                            expected.add(value);
                        }
                    }
                }
            }
        }
        if (expected.contains(v) == false) {
            throw new AssertionError(
                (expected.size() == 1 ? "expected " + expected.iterator().next() : "expected one of " + expected.stream().sorted().toList())
                    + " but was "
                    + v
            );
        }
    }
}
