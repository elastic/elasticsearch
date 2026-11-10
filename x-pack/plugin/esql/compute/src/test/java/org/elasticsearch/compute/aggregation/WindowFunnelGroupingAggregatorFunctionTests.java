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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

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

    public void testCompleteGroupsCollapseAcrossStages() {
        DriverContext driverContext = driverContext();
        try (
            WindowFunnelAggregator.GroupingState partial = WindowFunnelAggregator.initGrouping(driverContext, 10, 3);
            WindowFunnelAggregator.GroupingState merged = WindowFunnelAggregator.initGrouping(driverContext, 10, 3);
            IntVector selected = driverContext.blockFactory().newIntRangeVector(0, 4);
            GroupingAggregatorEvaluationContext ctx = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            WindowFunnelAggregator.combine(partial, 0, 1, 0b001);
            WindowFunnelAggregator.combine(partial, 0, 2, 0b010);
            WindowFunnelAggregator.combine(partial, 0, 3, 0b100);
            WindowFunnelAggregator.combine(partial, 0, 4, 0b001);
            for (long timestamp = 1; timestamp <= 4; timestamp++) {
                WindowFunnelAggregator.combine(partial, 1, timestamp, 0b001);
            }
            WindowFunnelAggregator.combine(partial, 2, 1, 0b011);
            WindowFunnelAggregator.combine(partial, 2, 2, 0b100);
            WindowFunnelAggregator.combine(partial, 3, 7, 0b111);
            WindowFunnelAggregator.combine(partial, 3, 8, 0b001);

            Block[] intermediate = evaluate(partial, selected, ctx, false);
            try (Releasable release = Releasables.wrap(intermediate)) {
                LongBlock timestamps = (LongBlock) intermediate[0];
                LongBlock masks = (LongBlock) intermediate[1];
                assertThat(BlockUtils.toJavaObject(masks, 0), equalTo(0b111L));
                assertThat(BlockUtils.toJavaObject(masks, 1), equalTo(List.of(0b001L, 0b001L, 0b001L, 0b001L)));
                assertThat(BlockUtils.toJavaObject(masks, 2), equalTo(0b111L));
                assertThat(BlockUtils.toJavaObject(masks, 3), equalTo(0b111L));
                for (int group = 0; group < 4; group++) {
                    WindowFunnelAggregator.combineIntermediate(merged, group, timestamps, masks, group);
                }
            }
            WindowFunnelAggregator.combine(merged, 0, 100, 0b001);
            WindowFunnelAggregator.combine(merged, 1, 5, 0b010);

            Block[] mergedIntermediate = evaluate(merged, selected, ctx, false);
            Block[] levels = evaluate(merged, selected, ctx, true);
            try (Releasable release = Releasables.wrap(Releasables.wrap(mergedIntermediate), Releasables.wrap(levels))) {
                assertThat(BlockUtils.toJavaObject(mergedIntermediate[1], 0), equalTo(0b111L));
                assertThat(BlockUtils.toJavaObject(levels[0], 0), equalTo(3));
                assertThat(BlockUtils.toJavaObject(levels[0], 1), equalTo(2));
                assertThat(BlockUtils.toJavaObject(levels[0], 2), equalTo(3));
                assertThat(BlockUtils.toJavaObject(levels[0], 3), equalTo(3));
            }
        }
    }

    public void testGroupsThatSeeEveryStepWithoutAChainAreNotCollapsed() {
        DriverContext driverContext = driverContext();
        try (
            WindowFunnelAggregator.GroupingState state = WindowFunnelAggregator.initGrouping(driverContext, 10, 3);
            IntVector selected = driverContext.blockFactory().newIntRangeVector(0, 3);
            GroupingAggregatorEvaluationContext ctx = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            WindowFunnelAggregator.combine(state, 0, 1, 0b100);
            WindowFunnelAggregator.combine(state, 0, 2, 0b010);
            WindowFunnelAggregator.combine(state, 0, 3, 0b001);
            WindowFunnelAggregator.combine(state, 1, 1, 0b001);
            WindowFunnelAggregator.combine(state, 1, 5, 0b010);
            WindowFunnelAggregator.combine(state, 1, 20, 0b100);
            WindowFunnelAggregator.combine(state, 2, 5, 0b011);
            WindowFunnelAggregator.combine(state, 2, 5, 0b100);
            WindowFunnelAggregator.combine(state, 2, 6, 0b001);

            Block[] intermediate = evaluate(state, selected, ctx, false);
            Block[] levels = evaluate(state, selected, ctx, true);
            try (Releasable release = Releasables.wrap(Releasables.wrap(intermediate), Releasables.wrap(levels))) {
                LongBlock masks = (LongBlock) intermediate[1];
                assertThat(masks.getValueCount(0), equalTo(3));
                assertThat(masks.getValueCount(1), equalTo(3));
                assertThat(BlockUtils.toJavaObject(masks, 2), equalTo(0b111L));
                assertThat(BlockUtils.toJavaObject(levels[0], 0), equalTo(1));
                assertThat(BlockUtils.toJavaObject(levels[0], 1), equalTo(2));
                assertThat(BlockUtils.toJavaObject(levels[0], 2), equalTo(3));
            }
        }
        try (WindowFunnelAggregator.GroupingState zeroSpan = WindowFunnelAggregator.initGrouping(driverContext, 0, 3)) {
            WindowFunnelAggregator.combine(zeroSpan, 0, 5, 0b011);
            WindowFunnelAggregator.combine(zeroSpan, 0, 5, 0b100);
            try (
                IntVector selected = driverContext.blockFactory().newIntRangeVector(0, 1);
                GroupingAggregatorEvaluationContext ctx = new GroupingAggregatorEvaluationContext(driverContext)
            ) {
                Block[] levels = evaluate(zeroSpan, selected, ctx, true);
                try (Releasable release = Releasables.wrap(levels)) {
                    assertThat(BlockUtils.toJavaObject(levels[0], 0), equalTo(3));
                }
            }
        }
    }

    private static Block[] evaluate(
        WindowFunnelAggregator.GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx,
        boolean last
    ) {
        Block[] blocks = new Block[last ? 1 : 2];
        try (
            var prepared = last
                ? WindowFunnelAggregator.prepareEvaluateFinal(state, selected, ctx)
                : WindowFunnelAggregator.prepareEvaluateIntermediate(state, selected, ctx)
        ) {
            prepared.evaluate(blocks, 0, selected);
        }
        return blocks;
    }

    record FunnelInput(long span, int steps, boolean narrow, int allStepsOneIn) {
        static FunnelInput random() {
            int steps = between(1, 32);
            int allStepsOneIn = randomFrom(0, 10, 1_000, 50_000);
            if (randomBoolean()) {
                return new FunnelInput(randomLongBetween(0, 30), steps, true, allStepsOneIn);
            }
            return new FunnelInput(randomFrom(0L, Long.MAX_VALUE, randomNonNegativeLong()), steps, false, allStepsOneIn);
        }

        long timestamp() {
            return narrow ? randomLongBetween(0, 100) : randomLong();
        }

        long mask() {
            if (allStepsOneIn > 0 && between(1, allStepsOneIn) == 1) {
                return (1L << steps) - 1;
            }
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
