/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.ForkingOperatorTestCase;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.NullInsertingSourceOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PositionMergingSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockTestUtils.append;
import static org.elasticsearch.compute.data.BlockTestUtils.randomValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class GroupingAggregatorFunctionTestCase extends ForkingOperatorTestCase {
    protected abstract AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, int inputChannel);

    protected abstract String expectedDescriptionOfAggregator();

    protected abstract void assertSimpleGroup(List<Page> input, Block result, int position, long group);

    @Override
    protected Operator.OperatorFactory simpleWithMode(BigArrays bigArrays, AggregatorMode mode) {
        return new HashAggregationOperator.HashAggregationOperatorFactory(
            List.of(new HashAggregationOperator.GroupSpec(0, ElementType.LONG)),
            List.of(aggregatorFunction(bigArrays, 1).groupingAggregatorFactory(mode, 1)),
            bigArrays
        );
    }

    @Override
    protected final String expectedDescriptionOfSimple() {
        return "HashAggregationOperator[mode = <not-needed>, aggs = " + expectedDescriptionOfAggregator() + "]";
    }

    @Override
    protected final String expectedToStringOfSimple() {
        String type = getClass().getSimpleName().replace("Tests", "");
        return "HashAggregationOperator[blockHash=LongBlockHash{channel=0, entries=0}, aggregators=[GroupingAggregator[aggregatorFunction="
            + type
            + "[channel=1], mode=SINGLE]]]";
    }

    @Override
    protected final void assertSimpleOutput(List<Page> input, List<Page> results) {
        SortedSet<Long> seenGroups = new TreeSet<>();
        for (Page in : input) {
            LongBlock groups = in.getBlock(0);
            for (int p = 0; p < in.getPositionCount(); p++) {
                if (groups.isNull(p)) {
                    continue;
                }
                int start = groups.getFirstValueIndex(p);
                int end = start + groups.getValueCount(p);
                for (int g = start; g < end; g++) {
                    seenGroups.add(groups.getLong(g));
                }
            }
        }

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(2));
        assertThat(results.get(0).getPositionCount(), equalTo(seenGroups.size()));

        LongBlock groups = results.get(0).getBlock(0);
        Block result = results.get(0).getBlock(1);
        for (int i = 0; i < seenGroups.size(); i++) {
            long group = groups.getLong(i);
            assertSimpleGroup(input, result, i, group);
        }
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        return ByteSizeValue.ofBytes(between(1, 32));
    }

    public final void testIgnoresNullGroupsAndValues() {
        DriverContext driverContext = new DriverContext();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(new NullInsertingSourceOperator(simpleInput(end)));
        List<Page> results = drive(simple(nonBreakingBigArrays().withCircuitBreaking()).get(driverContext), input.iterator());
        assertSimpleOutput(input, results);
    }

    public final void testIgnoresNullGroups() {
        DriverContext driverContext = new DriverContext();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(nullGroups(simpleInput(end)));
        List<Page> results = drive(simple(nonBreakingBigArrays().withCircuitBreaking()).get(driverContext), input.iterator());
        assertSimpleOutput(input, results);
    }

    private SourceOperator nullGroups(SourceOperator source) {
        return new NullInsertingSourceOperator(source) {
            @Override
            protected void appendNull(ElementType elementType, Block.Builder builder, int blockId) {
                if (blockId == 0) {
                    super.appendNull(elementType, builder, blockId);
                } else {
                    append(builder, randomValue(elementType));
                }
            }
        };
    }

    public final void testIgnoresNullValues() {
        DriverContext driverContext = new DriverContext();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(nullValues(simpleInput(end)));
        List<Page> results = drive(simple(nonBreakingBigArrays().withCircuitBreaking()).get(driverContext), input.iterator());
        assertSimpleOutput(input, results);
    }

    private SourceOperator nullValues(SourceOperator source) {
        return new NullInsertingSourceOperator(source) {
            @Override
            protected void appendNull(ElementType elementType, Block.Builder builder, int blockId) {
                if (blockId == 0) {
                    ((LongBlock.Builder) builder).appendLong(between(0, 4));
                } else {
                    super.appendNull(elementType, builder, blockId);
                }
            }
        };
    }

    public final void testMultivalued() {
        DriverContext driverContext = new DriverContext();
        int end = between(1_000, 100_000);
        List<Page> input = CannedSourceOperator.collectPages(mergeValues(simpleInput(end)));
        List<Page> results = drive(simple(nonBreakingBigArrays().withCircuitBreaking()).get(driverContext), input.iterator());
        assertSimpleOutput(input, results);
    }

    public final void testMulitvaluedIgnoresNullGroupsAndValues() {
        DriverContext driverContext = new DriverContext();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(new NullInsertingSourceOperator(mergeValues(simpleInput(end))));
        List<Page> results = drive(simple(nonBreakingBigArrays().withCircuitBreaking()).get(driverContext), input.iterator());
        assertSimpleOutput(input, results);
    }

    public final void testMulitvaluedIgnoresNullGroups() {
        DriverContext driverContext = new DriverContext();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(nullGroups(mergeValues(simpleInput(end))));
        List<Page> results = drive(simple(nonBreakingBigArrays().withCircuitBreaking()).get(driverContext), input.iterator());
        assertSimpleOutput(input, results);
    }

    public final void testMulitvaluedIgnoresNullValues() {
        DriverContext driverContext = new DriverContext();
        int end = between(50, 60);
        List<Page> input = CannedSourceOperator.collectPages(nullValues(mergeValues(simpleInput(end))));
        List<Page> results = drive(simple(nonBreakingBigArrays().withCircuitBreaking()).get(driverContext), input.iterator());
        assertSimpleOutput(input, results);
    }

    private SourceOperator mergeValues(SourceOperator orig) {
        return new PositionMergingSourceOperator(orig) {
            @Override
            protected Block merge(int blockIndex, Block block) {
                // Merge positions for all blocks but the first. For the first just take the first position.
                if (blockIndex != 0) {
                    return super.merge(blockIndex, block);
                }
                Block.Builder builder = block.elementType().newBlockBuilder(block.getPositionCount() / 2);
                for (int p = 0; p + 1 < block.getPositionCount(); p += 2) {
                    builder.copyFrom(block, p, p + 1);
                }
                if (block.getPositionCount() % 2 == 1) {
                    builder.copyFrom(block, block.getPositionCount() - 1, block.getPositionCount());
                }
                return builder.build();
            }
        };
    }

    protected static IntStream allValueOffsets(Page page, long group) {
        LongBlock groupBlock = page.getBlock(0);
        Block valueBlock = page.getBlock(1);
        return IntStream.range(0, page.getPositionCount()).flatMap(p -> {
            if (groupBlock.isNull(p) || valueBlock.isNull(p)) {
                return IntStream.of();
            }
            int groupStart = groupBlock.getFirstValueIndex(p);
            int groupEnd = groupStart + groupBlock.getValueCount(p);
            boolean matched = false;
            for (int i = groupStart; i < groupEnd; i++) {
                if (groupBlock.getLong(i) == group) {
                    matched = true;
                    break;
                }
            }
            if (matched == false) {
                return IntStream.of();
            }
            int start = valueBlock.getFirstValueIndex(p);
            int end = start + valueBlock.getValueCount(p);
            return IntStream.range(start, end);
        });
    }

    protected static Stream<BytesRef> allBytesRefs(Page page, long group) {
        BytesRefBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToObj(i -> b.getBytesRef(i, new BytesRef()));
    }

    protected static Stream<Boolean> allBooleans(Page page, long group) {
        BooleanBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToObj(i -> b.getBoolean(i));
    }

    protected static DoubleStream allDoubles(Page page, long group) {
        DoubleBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToDouble(i -> b.getDouble(i));
    }

    protected static IntStream allInts(Page page, long group) {
        IntBlock b = page.getBlock(1);
        return allValueOffsets(page, group).map(i -> b.getInt(i));
    }

    protected static LongStream allLongs(Page page, long group) {
        LongBlock b = page.getBlock(1);
        return allValueOffsets(page, group).mapToLong(i -> b.getLong(i));
    }

}
