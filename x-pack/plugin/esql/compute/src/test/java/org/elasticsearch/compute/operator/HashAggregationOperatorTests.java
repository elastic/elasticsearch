/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxLongGroupingAggregatorFunctionTests;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongGroupingAggregatorFunctionTests;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.LongStream;

import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class HashAggregationOperatorTests extends ForkingOperatorTestCase {
    private int maxPageSize;

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size);
        return new TupleLongLongBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(l % 5, randomLongBetween(-max, max)))
        );
    }

    @Override
    protected Operator.OperatorFactory simpleWithMode(SimpleOptions options, AggregatorMode mode) {
        List<Integer> sumChannels, maxChannels;
        if (mode.isInputPartial()) {
            int sumChannelCount = SumLongAggregatorFunction.intermediateStateDesc().size();
            int maxChannelCount = MaxLongAggregatorFunction.intermediateStateDesc().size();
            sumChannels = range(1, 1 + sumChannelCount).boxed().toList();
            maxChannels = range(1 + sumChannelCount, 1 + sumChannelCount + maxChannelCount).boxed().toList();
        } else {
            sumChannels = maxChannels = List.of(1);
        }

        maxPageSize = randomPageSize();
        return new HashAggregationOperator.HashAggregationOperatorFactory(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG)),
            mode,
            List.of(
                new SumLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, sumChannels),
                new MaxLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, maxChannels)
            ),
            maxPageSize,
            randomPageSize(),
            null
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("HashAggregationOperator[mode = <not-needed>, aggs = sum of longs, max of longs]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "HashAggregationOperator[blockHash=LongBlockHash{channel=0, entries=0, seenNull=false}, aggregators=["
                + "GroupingAggregator[aggregatorFunction=SumLongGroupingAggregatorFunction[channels=[1]], mode=SINGLE], "
                + "GroupingAggregator[aggregatorFunction=MaxLongGroupingAggregatorFunction[channels=[1]], mode=SINGLE]]]"
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int totalPositions = 0;
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(3));
            assertThat(page.getPositionCount(), lessThanOrEqualTo(maxPageSize));
            totalPositions += page.getPositionCount();
        }
        assertThat(totalPositions, equalTo(5));

        SumLongGroupingAggregatorFunctionTests sum = new SumLongGroupingAggregatorFunctionTests();
        MaxLongGroupingAggregatorFunctionTests max = new MaxLongGroupingAggregatorFunctionTests();

        List<Long> seenGroups = new ArrayList<>();
        for (Page page : results) {
            LongBlock groups = page.getBlock(0);
            Block sums = page.getBlock(1);
            Block maxes = page.getBlock(2);
            for (int i = 0; i < groups.getPositionCount(); i++) {
                long group = groups.getLong(i);
                seenGroups.add(group);
                sum.assertSimpleGroup(input, sums, i, group);
                max.assertSimpleGroup(input, maxes, i, group);
            }
        }
        Collections.sort(seenGroups);
        assertThat(seenGroups, equalTo(List.of(0L, 1L, 2L, 3L, 4L)));
    }

    public void testTopNNullsLast() {
        boolean ascOrder = randomBoolean();
        var groups = new Long[] { 0L, 10L, 20L, 30L, 40L, 50L };
        if (ascOrder) {
            Arrays.sort(groups, Comparator.reverseOrder());
        }
        var mode = AggregatorMode.SINGLE;
        var groupChannel = 0;
        var aggregatorChannels = List.of(1);
        int maxPageSize = randomPageSize();

        try (
            var operator = new HashAggregationOperator.HashAggregationOperatorFactory(
                List.of(new BlockHash.GroupSpec(groupChannel, ElementType.LONG, null, new BlockHash.TopNDef(0, ascOrder, false, 3))),
                mode,
                List.of(
                    new SumLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, aggregatorChannels),
                    new MaxLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, aggregatorChannels)
                ),
                maxPageSize,
                randomPageSize(),
                null
            ).get(driverContext())
        ) {
            var page = new Page(
                BlockUtils.fromList(
                    blockFactory(),
                    List.of(
                        List.of(groups[1], 2L),
                        Arrays.asList(null, 1L),
                        List.of(groups[2], 4L),
                        List.of(groups[3], 8L),
                        List.of(groups[3], 16L)
                    )
                )
            );
            operator.addInput(page);

            page = new Page(
                BlockUtils.fromList(
                    blockFactory(),
                    List.of(
                        List.of(groups[5], 64L),
                        List.of(groups[4], 32L),
                        List.of(List.of(groups[1], groups[5]), 128L),
                        List.of(groups[0], 256L),
                        Arrays.asList(null, 512L)
                    )
                )
            );
            operator.addInput(page);
            assertTopN(
                maxPageSize,
                operator,
                equalTo(List.of(groups[3], groups[5], groups[4])),
                equalTo(List.of(24L, 192L, 32L)),
                equalTo(List.of(16L, 128L, 32L))
            );
        }
    }

    public void testTopNNullsFirst() {
        boolean ascOrder = randomBoolean();
        var groups = new Long[] { 0L, 10L, 20L, 30L, 40L, 50L };
        if (ascOrder) {
            Arrays.sort(groups, Comparator.reverseOrder());
        }
        var mode = AggregatorMode.SINGLE;
        var groupChannel = 0;
        var aggregatorChannels = List.of(1);
        int maxPageSize = randomPageSize();

        try (
            var operator = new HashAggregationOperator.HashAggregationOperatorFactory(
                List.of(new BlockHash.GroupSpec(groupChannel, ElementType.LONG, null, new BlockHash.TopNDef(0, ascOrder, true, 3))),
                mode,
                List.of(
                    new SumLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, aggregatorChannels),
                    new MaxLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, aggregatorChannels)
                ),
                maxPageSize,
                randomPageSize(),
                null
            ).get(driverContext())
        ) {
            var page = new Page(
                BlockUtils.fromList(
                    blockFactory(),
                    List.of(
                        List.of(groups[1], 2L),
                        Arrays.asList(null, 1L),
                        List.of(groups[2], 4L),
                        List.of(groups[3], 8L),
                        List.of(groups[3], 16L)
                    )
                )
            );
            operator.addInput(page);

            page = new Page(
                BlockUtils.fromList(
                    blockFactory(),
                    List.of(
                        List.of(groups[5], 64L),
                        List.of(groups[4], 32L),
                        List.of(List.of(groups[1], groups[5]), 128L),
                        List.of(groups[0], 256L),
                        Arrays.asList(null, 512L)
                    )
                )
            );
            operator.addInput(page);
            assertTopN(
                maxPageSize,
                operator,
                equalTo(Arrays.asList(null, groups[5], groups[4])),
                equalTo(List.of(513L, 192L, 32L)),
                equalTo(List.of(512L, 128L, 32L))
            );
        }
    }

    /**
     * When in intermediate/final mode, it will receive intermediate outputs that may have to be discarded
     * (TopN in the datanode but not acceptable in the coordinator).
     * <p>
     *     This test ensures that such discarding works correctly.
     * </p>
     */
    public void testTopNNullsIntermediateDiscards() {
        boolean ascOrder = randomBoolean();
        var groups = new Long[] { 0L, 10L, 20L, 30L, 40L, 50L };
        if (ascOrder) {
            Arrays.sort(groups, Comparator.reverseOrder());
        }
        var groupChannel = 0;
        int maxPageSize = randomPageSize();

        // Supplier of operators to ensure that they're identical, simulating a datanode/coordinator connection
        Function<AggregatorMode, Operator> makeAggWithMode = (mode) -> {
            var sumAggregatorChannels = mode.isInputPartial() ? List.of(1, 2) : List.of(1);
            var maxAggregatorChannels = mode.isInputPartial() ? List.of(3, 4) : List.of(1);

            return new HashAggregationOperator.HashAggregationOperatorFactory(
                List.of(new BlockHash.GroupSpec(groupChannel, ElementType.LONG, null, new BlockHash.TopNDef(0, ascOrder, false, 3))),
                mode,
                List.of(
                    new SumLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, sumAggregatorChannels),
                    new MaxLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, maxAggregatorChannels)
                ),
                maxPageSize,
                randomPageSize(),
                null
            ).get(driverContext());
        };

        // The operator that will collect all the results
        try (var collectingOperator = makeAggWithMode.apply(AggregatorMode.FINAL)) {
            // First datanode, sending a suitable TopN set of data
            try (var datanodeOperator = makeAggWithMode.apply(AggregatorMode.INITIAL)) {
                var page = new Page(
                    BlockUtils.fromList(blockFactory(), List.of(List.of(groups[4], 1L), List.of(groups[3], 2L), List.of(groups[2], 4L)))
                );
                datanodeOperator.addInput(page);
                datanodeOperator.finish();

                Page outputPage;
                while ((outputPage = datanodeOperator.getOutput()) != null) {
                    collectingOperator.addInput(outputPage);
                }
            }

            // Second datanode, sending an outdated TopN, as the coordinator has better top values already
            try (var datanodeOperator = makeAggWithMode.apply(AggregatorMode.INITIAL)) {
                var page = new Page(
                    BlockUtils.fromList(
                        blockFactory(),
                        List.of(
                            List.of(groups[5], 8L),
                            List.of(groups[3], 16L),
                            List.of(groups[1], 32L) // This group is worse than the worst group in the coordinator
                        )
                    )
                );
                datanodeOperator.addInput(page);
                datanodeOperator.finish();

                Page outputPage;
                while ((outputPage = datanodeOperator.getOutput()) != null) {
                    collectingOperator.addInput(outputPage);
                }
            }

            assertTopN(
                maxPageSize,
                collectingOperator,
                equalTo(List.of(groups[4], groups[3], groups[5])),
                equalTo(List.of(1L, 18L, 8L)),
                equalTo(List.of(1L, 16L, 8L))
            );
        }
    }

    private void assertTopN(
        int maxPageSize,
        Operator operator,
        Matcher<List<Long>> expectedGroups,
        Matcher<List<Long>> expectedSums,
        Matcher<List<Long>> expectedMaxes
    ) {
        operator.finish();

        List<Long> seenGroups = new ArrayList<>();
        List<Long> seenSums = new ArrayList<>();
        List<Long> seenMaxes = new ArrayList<>();
        Page outputPage;
        while ((outputPage = operator.getOutput()) != null) {
            assertThat(outputPage.getBlockCount(), equalTo(3));
            assertThat(outputPage.getPositionCount(), lessThanOrEqualTo(maxPageSize));
            LongBlock groupsBlock = outputPage.getBlock(0);
            LongVector sumBlock = outputPage.<LongBlock>getBlock(1).asVector();
            LongVector maxBlock = outputPage.<LongBlock>getBlock(2).asVector();

            for (int i = 0; i < groupsBlock.getPositionCount(); i++) {
                if (groupsBlock.isNull(i)) {
                    seenGroups.add(null);
                } else {
                    assertThat(groupsBlock.getValueCount(i), equalTo(1));
                    seenGroups.add(groupsBlock.getLong(groupsBlock.getFirstValueIndex(i)));
                }
                seenSums.add(sumBlock.getLong(i));
                seenMaxes.add(maxBlock.getLong(i));
            }
            outputPage.releaseBlocks();
        }
        assertThat(seenGroups, expectedGroups);
        assertThat(seenSums, expectedSums);
        assertThat(seenMaxes, expectedMaxes);
    }
}
