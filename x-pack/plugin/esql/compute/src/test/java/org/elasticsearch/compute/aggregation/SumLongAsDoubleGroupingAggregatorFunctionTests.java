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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SumLongAsDoubleGroupingAggregatorFunctionTests extends PartitionedGroupingAggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        // Any long will do: the sum of them doesn't fit in a long, and that is the point.
        return new TupleLongLongBlockSourceOperator(
            blockFactory,
            LongStream.range(0, end).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomLong()))
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SumLongAsDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of longs as doubles";
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
                assertThat((Double) partitioned.value(), closeTo(value, tolerance(value)));
            }
        }
    }

    /**
     * With groups this is {@code SUM(TO_DOUBLE(x))}: the compensated sum of the values as doubles.
     */
    /**
     * Partitioning changes which pages hold a single group, and those are summed exactly while the others convert every long to a
     * {@code double} first, so the results can differ in the last digits.
     */
    @Override
    protected void assertTimeSeriesResults(List<TimeSeriesAggResult> singlePassResult, List<TimeSeriesAggResult> partitioningResult) {
        assertThat(partitioningResult.size(), equalTo(singlePassResult.size()));
        for (int i = 0; i < singlePassResult.size(); i++) {
            TimeSeriesAggResult singlePass = singlePassResult.get(i);
            TimeSeriesAggResult partitioned = partitioningResult.get(i);
            assertThat(partitioned.tsid(), equalTo(singlePass.tsid()));
            assertThat(partitioned.timestamp(), equalTo(singlePass.timestamp()));
            double expected = (Double) singlePass.value();
            // A series has at most two longs. Converting a long to a double is off by at most half an ulp of 2^63, and so is the
            // rounding of a sum, so a few of those is all the difference there can be.
            assertThat(partitioned.value().toString(), (Double) partitioned.value(), closeTo(expected, 2 * Math.ulp(0x1p63)));
        }
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        CompensatedSum sum = new CompensatedSum();
        input.stream().flatMapToLong(p -> allLongs(p, group)).forEach(v -> sum.add((double) v));
        // Won't precisely match in distributed case but will be close
        assertThat(((DoubleBlock) result).getDouble(position), closeTo(sum.value(), tolerance(sum.value())));
    }

    /**
     * Group ids that are the same for a whole page, like the ones of a grouping key that is constant in the page, take
     * a path of their own which adds the values of the page before they go into the sum of the group.
     */
    public void testConstantGroupIds() {
        long[] values = LongStream.generate(() -> randomBoolean() ? randomLong() : randomLongBetween(-1000, 1000))
            .limit(between(2, 20_000))
            .toArray();
        // Two runs of the same page, each for one group, so that the second starts in the middle of the values.
        int split = between(1, values.length - 1);
        double[] sums = sumConstantGroups(values, split);
        assertThat(sums[0], equalTo(exactSum(values, 0, split)));
        assertThat(sums[1], equalTo(exactSum(values, split, values.length)));
    }

    /**
     * The sum of the values of a page of one group is exact. Adding them one by one as {@code double}s loses the
     * {@code 511} of every pair here, which is less than half of the distance between the {@code double}s around
     * {@code 2^62}, and then the pairs cancel out to {@code 0}.
     */
    public void testConstantGroupIdsAreAddedExactly() {
        int pairs = between(1, 1000);
        long[] values = new long[pairs * 2];
        for (int i = 0; i < pairs; i++) {
            values[2 * i] = (1L << 62) + 511;
            values[2 * i + 1] = -(1L << 62);
        }
        double[] sums = sumConstantGroups(values, values.length);
        assertThat(sums[0], equalTo(511.0 * pairs));
    }

    /**
     * A page of one group that sums to a negative number is added to a group that already has a sum. The small sum of the group
     * must survive: it is not allowed to be rounded away by a large part of the sum of the page.
     */
    public void testConstantGroupPageAfterAnotherPage() {
        assertThat(sumOfConstantGroupPages(new long[] { 1, 2 }, new long[] { -1, -2 }), equalTo(0.0));
        // The second page is 2^54 + 1, which is a value and a correction. The value cancels the sum of the group, then the correction
        // stays.
        assertThat(sumOfConstantGroupPages(new long[] { -(1L << 54), 0 }, new long[] { 1L << 54, 1 }), equalTo(1.0));
        // A power of two is a double, so the first page leaves no correction behind. A correction that the group already holds is
        // still combined with the next value by the shared compensated sum, which is the limit that is documented.
        // The sum of the group has a correction when the next page cancels its large part.
        assertThat(sumOfConstantGroupPages(new long[] { 1L << 62, 255 }, new long[] { -255, -(1L << 62) }), equalTo(0.0));
        long big = 1L << between(54, 60);
        assertThat(sumOfConstantGroupPages(new long[] { -big, 0 }, new long[] { big, 1 }), equalTo(1.0));
        long[] first = LongStream.generate(() -> randomLongBetween(-1000, 1000)).limit(between(1, 100)).toArray();
        long[] second = LongStream.generate(() -> randomLongBetween(-1000, 1000)).limit(between(1, 100)).toArray();
        long[] both = LongStream.concat(Arrays.stream(first), Arrays.stream(second)).toArray();
        assertThat(sumOfConstantGroupPages(first, second), equalTo(exactSum(both, 0, both.length)));
    }

    /** Adds each page to group {@code 0} with a constant vector of group ids and returns the sum of the group. */
    private double sumOfConstantGroupPages(long[]... pages) {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        try (
            GroupingAggregatorFunction agg = aggregatorFunction().groupingAggregator(driverContext, List.of(0));
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0 }, 1);
            GroupingAggregatorEvaluationContext evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            for (long[] values : pages) {
                try (
                    Page page = new Page(blockFactory.newLongArrayVector(values, values.length).asBlock());
                    GroupingAggregatorFunction.AddInput add = agg.prepareProcessRawInputPage(new SeenGroupIds.Empty(), page);
                    IntVector group = blockFactory.newConstantIntVector(0, values.length)
                ) {
                    add.add(0, group);
                }
            }
            Block[] blocks = new Block[1];
            try (GroupingAggregatorFunction.PreparedForEvaluation prepared = agg.prepareEvaluateFinal(selected, evalContext)) {
                prepared.evaluate(blocks, 0, selected);
            }
            try (DoubleBlock result = (DoubleBlock) blocks[0]) {
                return result.getDouble(0);
            }
        }
    }

    /**
     * Sums {@code values[0..split)} into one group and the rest into another, each with a constant vector of group ids.
     */
    private double[] sumConstantGroups(long[] values, int split) {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        int groupCount = split == values.length ? 1 : 2;
        try (
            GroupingAggregatorFunction agg = aggregatorFunction().groupingAggregator(driverContext, List.of(0));
            Page page = new Page(blockFactory.newLongArrayVector(values, values.length).asBlock());
            IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, groupCount);
            GroupingAggregatorEvaluationContext evalContext = new GroupingAggregatorEvaluationContext(driverContext)
        ) {
            try (GroupingAggregatorFunction.AddInput add = agg.prepareProcessRawInputPage(new SeenGroupIds.Empty(), page)) {
                try (IntVector first = blockFactory.newConstantIntVector(0, split)) {
                    add.add(0, first);
                }
                if (groupCount == 2) {
                    try (IntVector second = blockFactory.newConstantIntVector(1, values.length - split)) {
                        add.add(split, second);
                    }
                }
            }
            Block[] blocks = new Block[1];
            try (GroupingAggregatorFunction.PreparedForEvaluation prepared = agg.prepareEvaluateFinal(selected, evalContext)) {
                prepared.evaluate(blocks, 0, selected);
            }
            try (DoubleBlock result = (DoubleBlock) blocks[0]) {
                double[] sums = new double[groupCount];
                for (int g = 0; g < groupCount; g++) {
                    sums[g] = result.getDouble(g);
                }
                return sums;
            }
        }
    }

    /**
     * The sum that a group has after one page of its own: the exact sum of the values, rounded to a {@code double} once.
     */
    private static double exactSum(long[] values, int from, int to) {
        return Arrays.stream(values, from, to).mapToObj(BigInteger::valueOf).reduce(BigInteger.ZERO, BigInteger::add).doubleValue();
    }

    /**
     * The values are as large as {@code 2^63} and cancel each other out, so the error is relative to them and not to the sum.
     */
    private static double tolerance(double sum) {
        return Math.max(0x1p63, Math.abs(sum)) * 1e-9;
    }
}
