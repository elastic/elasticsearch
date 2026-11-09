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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class WindowFunnelGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    private final FunnelInput funnel = FunnelInput.random();

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.LONG, ElementType.LONG, ElementType.LONG),
            IntStream.range(0, size).mapToObj(l -> List.<Object>of(randomLongBetween(0, 4), funnel.timestamp(), funnel.mask())).toList()
        );
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new WindowFunnelAggregatorFunctionSupplier(funnel.span(), funnel.steps());
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "window_funnel";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        ExpectedFunnel expected = new ExpectedFunnel();
        for (Page page : input) {
            matchingGroups(page, group).forEach(p -> expected.add(page.getBlock(1), page.getBlock(2), p));
        }
        assertThat(BlockUtils.toJavaObject(result, position), equalTo(expected.level(funnel.span(), funnel.steps())));
    }

    record FunnelInput(long span, int steps, boolean narrow) {
        static FunnelInput random() {
            int steps = between(1, 32);
            if (randomBoolean()) {
                return new FunnelInput(randomLongBetween(0, 30), steps, true);
            }
            return new FunnelInput(randomFrom(0L, Long.MAX_VALUE, randomNonNegativeLong()), steps, false);
        }

        long timestamp() {
            return narrow ? randomLongBetween(0, 100) : randomLong();
        }

        long mask() {
            return randomBoolean() ? 1L << between(0, steps - 1) : randomLongBetween(0, (1L << steps) - 1);
        }
    }

    static class ExpectedFunnel {
        private final List<long[]> events = new ArrayList<>();

        void add(LongBlock timestamps, LongBlock masks, int position) {
            if (timestamps.isNull(position) || masks.isNull(position)) {
                return;
            }
            int tsStart = timestamps.getFirstValueIndex(position);
            int maskStart = masks.getFirstValueIndex(position);
            for (int t = tsStart; t < tsStart + timestamps.getValueCount(position); t++) {
                for (int m = maskStart; m < maskStart + masks.getValueCount(position); m++) {
                    add(timestamps.getLong(t), masks.getLong(m));
                }
            }
        }

        void add(long timestamp, long mask) {
            for (int step = 0; step < Long.SIZE; step++) {
                if ((mask & (1L << step)) != 0) {
                    events.add(new long[] { timestamp, step });
                }
            }
        }

        Integer level(long span, int steps) {
            if (events.isEmpty()) {
                return null;
            }
            long[][] timestampsByStep = new long[steps][];
            for (int step = 0; step < steps; step++) {
                int s = step;
                timestampsByStep[step] = events.stream().filter(e -> e[1] == s).mapToLong(e -> e[0]).sorted().distinct().toArray();
            }
            int best = 0;
            for (long start : timestampsByStep[0]) {
                BigInteger deadline = BigInteger.valueOf(start).add(BigInteger.valueOf(span));
                long previous = start;
                int level = 1;
                while (level < steps) {
                    long[] candidates = timestampsByStep[level];
                    int index = Arrays.binarySearch(candidates, previous);
                    if (index < 0) {
                        index = -1 - index;
                    }
                    if (index == candidates.length || BigInteger.valueOf(candidates[index]).compareTo(deadline) > 0) {
                        break;
                    }
                    previous = candidates[index];
                    level++;
                }
                best = Math.max(best, level);
            }
            return best;
        }
    }
}
