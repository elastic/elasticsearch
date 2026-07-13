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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

public class LastLongByTimestampGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        FirstLongByTimestampGroupingAggregatorFunctionTests.TimestampGen tsgen = randomFrom(
            FirstLongByTimestampGroupingAggregatorFunctionTests.TimestampGen.values()
        );
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.LONG, ElementType.LONG, ElementType.LONG),
            IntStream.range(0, size).mapToObj(l -> List.of(randomLongBetween(0, 4), randomLong(), tsgen.gen())).toList()
        );
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new LastLongByTimestampAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "last_long_by_timestamp";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        ExpectedWork work = new ExpectedWork(false);
        for (Page page : input) {
            matchingGroups(page, group).forEach(p -> {
                LongBlock values = page.getBlock(1);
                LongBlock timestamps = page.getBlock(2);
                int tsStart = timestamps.getFirstValueIndex(p);
                int tsEnd = tsStart + timestamps.getValueCount(p);
                for (int tsOffset = tsStart; tsOffset < tsEnd; tsOffset++) {
                    long timestamp = timestamps.getLong(tsOffset);
                    int vStart = values.getFirstValueIndex(p);
                    int vEnd = vStart + values.getValueCount(p);
                    for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
                        long value = values.getLong(vOffset);
                        work.add(timestamp, value);
                    }
                }
            });
        }
        work.check(BlockUtils.toJavaObject(result, position));
    }

    static class ExpectedWork {
        private final Set<Object> expected = new HashSet<>();
        private final boolean first;
        private long expectedTimestamp = 0;

        ExpectedWork(boolean first) {
            this.first = first;
        }

        void add(long timestamp, Object value) {
            if (expected.isEmpty()) {
                expectedTimestamp = timestamp;
                expected.add(value);
            } else if (first ? timestamp < expectedTimestamp : timestamp > expectedTimestamp) {
                expectedTimestamp = timestamp;
                expected.clear();
                expected.add(value);
            } else if (timestamp == expectedTimestamp) {
                expected.add(value);
            }
        }

        void check(Object v) {
            if (expected.isEmpty()) {
                if (v != null) {
                    throw new AssertionError("expected null but was " + v);
                }
            } else {
                if (expected.contains(v) == false) {
                    String expectedMessage = expected.size() == 1
                        ? "expected " + expected.iterator().next()
                        : "expected one of " + expected.stream().sorted().toList();
                    throw new AssertionError(expectedMessage + " but was " + v);
                }
            }
        }
    }
}
