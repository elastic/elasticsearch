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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.LongStream;

import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class HashAggregationOperatorTests extends ForkingOperatorTestCase {
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

        return new HashAggregationOperator.HashAggregationOperatorFactory(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG)),
            mode,
            List.of(
                new SumLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, sumChannels),
                new MaxLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, maxChannels)
            ),
            randomPageSize(),
            between(1, 1000),
            randomDoubleBetween(0.1, 10.0, true),
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
        // With partitioning, results may be spread across multiple pages
        int totalPositions = results.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(totalPositions, equalTo(5));

        // Verify each page has a valid and unique partition ID
        Set<Integer> seenPartitionIds = new HashSet<>();
        for (Page page : results) {
            int partitionId = page.getPartitionId();
            assertThat("partitionId should be in [0, 256)", partitionId, greaterThanOrEqualTo(0));
            assertThat("partitionId should be in [0, 256)", partitionId, lessThan(256));
            assertTrue("duplicate partitionId " + partitionId, seenPartitionIds.add(partitionId));
        }

        SumLongGroupingAggregatorFunctionTests sum = new SumLongGroupingAggregatorFunctionTests();
        MaxLongGroupingAggregatorFunctionTests max = new MaxLongGroupingAggregatorFunctionTests();

        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(3));
            LongBlock groups = page.getBlock(0);
            Block sums = page.getBlock(1);
            Block maxs = page.getBlock(2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                long group = groups.getLong(i);
                sum.assertSimpleGroup(input, sums, i, group);
                max.assertSimpleGroup(input, maxs, i, group);
            }
        }
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

        try (
            var operator = new HashAggregationOperator.HashAggregationOperatorFactory(
                List.of(new BlockHash.GroupSpec(groupChannel, ElementType.LONG, null, new BlockHash.TopNDef(0, ascOrder, false, 3))),
                mode,
                List.of(
                    new SumLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, aggregatorChannels),
                    new MaxLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, aggregatorChannels)
                ),
                randomPageSize(),
                between(1, 1000),
                randomDoubleBetween(0.1, 10.0, true),
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

            operator.finish();

            Map<Long, long[]> collected = collectByRow(drainOutput(operator));

            assertThat(collected.size(), equalTo(3));
            assertThat(collected.get(groups[3]), equalTo(new long[] { 24L, 16L }));
            assertThat(collected.get(groups[5]), equalTo(new long[] { 192L, 128L }));
            assertThat(collected.get(groups[4]), equalTo(new long[] { 32L, 32L }));
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

        try (
            var operator = new HashAggregationOperator.HashAggregationOperatorFactory(
                List.of(new BlockHash.GroupSpec(groupChannel, ElementType.LONG, null, new BlockHash.TopNDef(0, ascOrder, true, 3))),
                mode,
                List.of(
                    new SumLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, aggregatorChannels),
                    new MaxLongAggregatorFunctionSupplier().groupingAggregatorFactory(mode, aggregatorChannels)
                ),
                randomPageSize(),
                between(1, 1000),
                randomDoubleBetween(0.1, 10.0, true),
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

            operator.finish();

            // Use null key in the map for the null group
            Map<Long, long[]> collected = collectByRow(drainOutput(operator));

            assertThat(collected.size(), equalTo(3));
            assertThat(collected.get(null), equalTo(new long[] { 513L, 512L }));
            assertThat(collected.get(groups[5]), equalTo(new long[] { 192L, 128L }));
            assertThat(collected.get(groups[4]), equalTo(new long[] { 32L, 32L }));
        }
    }

    /**
     * Drains all available output pages from an operator, releasing them from the operator's internal queue.
     */
    private static List<Page> drainOutput(Operator operator) {
        List<Page> pages = new ArrayList<>();
        Page p;
        while ((p = operator.getOutput()) != null) {
            pages.add(p);
        }
        return pages;
    }

    /**
     * Collects test results from (potentially multiple) output pages into a map from group key to [sum, max].
     * Uses {@code null} as the map key for null group values.
     * Pages are released after collection.
     */
    private static Map<Long, long[]> collectByRow(List<Page> pages) {
        Map<Long, long[]> results = new HashMap<>();
        for (Page page : pages) {
            LongBlock groupsBlock = page.getBlock(0);
            LongBlock sumBlock = page.getBlock(1);
            LongBlock maxBlock = page.getBlock(2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                Long group = groupsBlock.isNull(i) ? null : groupsBlock.getLong(groupsBlock.getFirstValueIndex(i));
                long sum = sumBlock.getLong(sumBlock.getFirstValueIndex(i));
                long max = maxBlock.getLong(maxBlock.getFirstValueIndex(i));
                results.put(group, new long[] { sum, max });
            }
            page.releaseBlocks();
        }
        return results;
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
                randomPageSize(),
                between(1, 1000),
                randomDoubleBetween(0.1, 10.0, true),
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

                // Drain all partitioned output pages and feed to the collecting operator
                for (Page outputPage : drainOutput(datanodeOperator)) {
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

                // Drain all partitioned output pages and feed to the collecting operator
                for (Page outputPage : drainOutput(datanodeOperator)) {
                    collectingOperator.addInput(outputPage);
                }
            }

            collectingOperator.finish();

            Map<Long, long[]> collected = collectByRow(drainOutput(collectingOperator));

            assertThat(collected.size(), equalTo(3));
            assertThat(collected.get(groups[4]), equalTo(new long[] { 1L, 1L }));
            assertThat(collected.get(groups[3]), equalTo(new long[] { 18L, 16L }));
            assertThat(collected.get(groups[5]), equalTo(new long[] { 8L, 8L }));
        }
    }
}
