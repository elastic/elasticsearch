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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntConsumer;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class DeltaDoubleAggregatorTests extends ComputeTestCase {
    private record Sample(long timestamp, double value) {}

    private static final long RANGE_START = 0;
    private static final long RANGE_END = 200_000;
    private static final double EVENLY_SPACED_DELTA = 9.9;

    /**
     * Ten samples, one every 10s, from 100s to 190s with values 0..9, in the window [0s, 200s]. Both gaps to the window
     * exceed 1.1 times the average sample interval (90s / 10 = 9s), so both are capped at 4.5s, giving
     * {@code 9 + 0.1 * 4.5 + 0.1 * 4.5}. Miscounting a single sample changes the interval to 10s and the result to 10.5.
     */
    private static List<Sample> evenlySpacedSamples() {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            samples.add(new Sample(100_000 + i * 10_000L, i));
        }
        return samples;
    }

    public void testRawInputInDescendingTimestampOrder() {
        List<Sample> samples = evenlySpacedSamples();
        Collections.reverse(samples);
        assertThat(evaluate(List.of(samples)), closeTo(EVENLY_SPACED_DELTA, 1e-9));
    }

    public void testRawInputCountsSamplesBetweenFirstAndLast() {
        List<Sample> samples = evenlySpacedSamples();
        List<Sample> outOfOrder = new ArrayList<>();
        outOfOrder.add(samples.get(0));
        outOfOrder.add(samples.get(9));
        outOfOrder.addAll(samples.subList(1, 9));
        assertThat(evaluate(List.of(outOfOrder)), closeTo(EVENLY_SPACED_DELTA, 1e-9));
    }

    public void testCombineEarlierPartialBeforeLaterPartial() {
        List<Sample> samples = evenlySpacedSamples();
        List<Sample> earlier = descending(samples.subList(0, 5));
        List<Sample> later = descending(samples.subList(5, 10));
        assertThat(evaluate(List.of(earlier, later)), closeTo(EVENLY_SPACED_DELTA, 1e-9));
    }

    public void testCombineLaterPartialBeforeEarlierPartial() {
        List<Sample> samples = evenlySpacedSamples();
        List<Sample> earlier = descending(samples.subList(0, 5));
        List<Sample> later = descending(samples.subList(5, 10));
        assertThat(evaluate(List.of(later, earlier)), closeTo(EVENLY_SPACED_DELTA, 1e-9));
    }

    public void testCombineInterleavedPartials() {
        List<Sample> samples = evenlySpacedSamples();
        List<Sample> even = new ArrayList<>();
        List<Sample> odd = new ArrayList<>();
        for (int i = 0; i < samples.size(); i++) {
            (i % 2 == 0 ? even : odd).add(samples.get(i));
        }
        assertThat(evaluate(List.of(descending(even), descending(odd))), closeTo(EVENLY_SPACED_DELTA, 1e-9));
        assertThat(evaluate(List.of(descending(odd), descending(even))), closeTo(EVENLY_SPACED_DELTA, 1e-9));
    }

    public void testMultivaluedSamplesAtFirstAndLastTimestampDoNotDependOnOrder() {
        List<Sample> samples = new ArrayList<>(evenlySpacedSamples());
        samples.add(new Sample(100_000, 7));
        samples.add(new Sample(100_000, -3));
        samples.add(new Sample(190_000, 20));
        samples.add(new Sample(190_000, 5));
        double expected = evaluate(List.of(samples));
        for (int i = 0; i < 20; i++) {
            List<Sample> shuffled = new ArrayList<>(samples);
            Collections.shuffle(shuffled, random());
            assertThat(evaluate(List.of(shuffled)), equalTo(expected));
        }
    }

    public void testPartialWithSingleMultivaluedSampleDoesNotDependOnOrder() {
        List<Sample> samples = evenlySpacedSamples();
        List<Sample> earlier = descending(samples.subList(0, 9));
        List<Sample> lastTimestamp = List.of(new Sample(190_000, 20), new Sample(190_000, 9), new Sample(190_000, 5));
        double expected = evaluate(List.of(lastTimestamp, earlier));
        assertThat(evaluate(List.of(earlier, lastTimestamp)), equalTo(expected));
        assertThat(
            evaluate(List.of(earlier, List.of(lastTimestamp.get(2), lastTimestamp.get(0), lastTimestamp.get(1)))),
            equalTo(expected)
        );
    }

    public void testRandomPartitionsMatchSingleState() {
        int sampleCount = between(2, 50);
        List<Sample> samples = new ArrayList<>(sampleCount);
        long timestamp = between(1, 50_000);
        for (int i = 0; i < sampleCount; i++) {
            int valuesAtTimestamp = randomBoolean() ? 1 : between(1, 4);
            for (int v = 0; v < valuesAtTimestamp; v++) {
                samples.add(new Sample(timestamp, randomIntBetween(-100, 100)));
            }
            timestamp += between(1, 3_000);
        }
        long rangeEnd = timestamp + between(0, 50_000);
        double expected = evaluate(List.of(samples), 0, rangeEnd);

        int partitionCount = between(1, 6);
        List<List<Sample>> partitions = new ArrayList<>();
        for (int p = 0; p < partitionCount; p++) {
            partitions.add(new ArrayList<>());
        }
        for (Sample sample : samples) {
            partitions.get(between(0, partitionCount - 1)).add(sample);
        }
        partitions.removeIf(List::isEmpty);
        for (List<Sample> partition : partitions) {
            Collections.shuffle(partition, random());
        }
        Collections.shuffle(partitions, random());
        assertThat(evaluate(partitions, 0, rangeEnd), equalTo(expected));
    }

    private static List<Sample> descending(List<Sample> samples) {
        List<Sample> copy = new ArrayList<>(samples);
        Collections.reverse(copy);
        return copy;
    }

    private double evaluate(List<List<Sample>> partitions) {
        return evaluate(partitions, RANGE_START, RANGE_END);
    }

    /**
     * Feeds each partition as raw input into its own state, then merges the partitions' intermediate states, in list
     * order, into a final state and evaluates it over {@code [rangeStart, rangeEnd]}.
     */
    private double evaluate(List<List<Sample>> partitions, long rangeStart, long rangeEnd) {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            var finalState = DeltaDoubleAggregator.initGrouping(driverContext);
            IntVector selected = blockFactory.newConstantIntVector(0, 1);
            var context = evaluationContext(driverContext, rangeStart, rangeEnd)
        ) {
            for (List<Sample> partition : partitions) {
                Block[] intermediate = new Block[3];
                try (var partialState = DeltaDoubleAggregator.initGrouping(driverContext)) {
                    for (Sample sample : partition) {
                        DeltaDoubleAggregator.combine(partialState, 0, sample.value(), sample.timestamp());
                    }
                    partialState.toIntermediate(intermediate, 0, selected, driverContext);
                    DeltaDoubleAggregator.combineIntermediate(
                        finalState,
                        0,
                        (LongBlock) intermediate[0],
                        (LongBlock) intermediate[1],
                        (DoubleBlock) intermediate[2],
                        0
                    );
                } finally {
                    Releasables.close(intermediate);
                }
            }
            try (DoubleBlock result = (DoubleBlock) DeltaDoubleAggregator.evaluateFinal(finalState, selected, context)) {
                assertThat(result.isNull(0), equalTo(false));
                return result.getDouble(0);
            }
        } finally {
            driverContext.finish();
        }
    }

    private static TimeSeriesGroupingAggregatorEvaluationContext evaluationContext(
        DriverContext driverContext,
        long rangeStart,
        long rangeEnd
    ) {
        return new TimeSeriesGroupingAggregatorEvaluationContext(driverContext) {
            @Override
            public long rangeStartInMillis(int groupId) {
                return rangeStart;
            }

            @Override
            public long rangeEndInMillis(int groupId) {
                return rangeEnd;
            }

            @Override
            public void forEachGroupInRange(int startingGroupId, long rangeStartMillis, long rangeEndMillis, IntConsumer action) {}

            @Override
            public int previousGroupId(int currentGroupId) {
                return -1;
            }

            @Override
            public int nextGroupId(int currentGroupId) {
                return -1;
            }

            @Override
            public void computeAdjacentGroupIds() {}
        };
    }
}
