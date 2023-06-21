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
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EmptySourceOperator;
import org.elasticsearch.compute.operator.ForkingOperatorTestCase;
import org.elasticsearch.compute.operator.NullInsertingSourceOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.PositionMergingSourceOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class AggregatorFunctionTestCase extends ForkingOperatorTestCase {
    protected abstract AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, List<Integer> inputChannels);

    protected abstract String expectedDescriptionOfAggregator();

    protected abstract void assertSimpleOutput(List<Block> input, Block result);

    @Override
    protected Operator.OperatorFactory simpleWithMode(BigArrays bigArrays, AggregatorMode mode) {
        return new AggregationOperator.AggregationOperatorFactory(
            List.of(aggregatorFunction(bigArrays, List.of(0)).aggregatorFactory(mode)),
            mode
        );
    }

    @Override
    protected final String expectedDescriptionOfSimple() {
        return "AggregationOperator[mode = SINGLE, aggs = " + expectedDescriptionOfAggregator() + "]";
    }

    @Override
    protected final String expectedToStringOfSimple() {
        String type = getClass().getSimpleName().replace("Tests", "");
        return "AggregationOperator[aggregators=[Aggregator[aggregatorFunction=" + type + "[channels=[0]], mode=SINGLE]]]";
    }

    @Override
    protected final void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(1));
        assertThat(results.get(0).getPositionCount(), equalTo(1));

        Block result = results.get(0).getBlock(0);
        assertSimpleOutput(input.stream().map(p -> p.<Block>getBlock(0)).toList(), result);
    }

    @Override
    protected final ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big array so never breaks", false);
        return null;
    }

    public final void testIgnoresNulls() {
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(end));
        DriverContext driverContext = new DriverContext();

        try (
            Driver d = new Driver(
                driverContext,
                new NullInsertingSourceOperator(new CannedSourceOperator(input.iterator())),
                List.of(simple(nonBreakingBigArrays().withCircuitBreaking()).get(driverContext)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(input, results);
    }

    public final void testMultivalued() {
        int end = between(1_000, 100_000);
        DriverContext driverContext = new DriverContext();
        List<Page> input = CannedSourceOperator.collectPages(new PositionMergingSourceOperator(simpleInput(end)));
        assertSimpleOutput(input, drive(simple(BigArrays.NON_RECYCLING_INSTANCE).get(driverContext), input.iterator()));
    }

    public final void testMultivaluedWithNulls() {
        int end = between(1_000, 100_000);
        DriverContext driverContext = new DriverContext();
        List<Page> input = CannedSourceOperator.collectPages(
            new NullInsertingSourceOperator(new PositionMergingSourceOperator(simpleInput(end)))
        );
        assertSimpleOutput(input, drive(simple(BigArrays.NON_RECYCLING_INSTANCE).get(driverContext), input.iterator()));
    }

    public final void testEmptyInput() {
        List<Page> results = new ArrayList<>();
        DriverContext driverContext = new DriverContext();

        try (
            Driver d = new Driver(
                driverContext,
                new EmptySourceOperator(),
                List.of(simple(nonBreakingBigArrays().withCircuitBreaking()).get(driverContext)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertThat(results, hasSize(1));
        assertOutputFromEmpty(results.get(0).getBlock(0));
    }

    /**
     * Asserts that the output from an empty input is a {@link Block} containing
     * only {@code null}. Override for {@code count} style aggregations that
     * return other sorts of results.
     */
    protected void assertOutputFromEmpty(Block b) {
        assertThat(b.elementType(), equalTo(ElementType.NULL));
        assertThat(b.getPositionCount(), equalTo(1));
        assertThat(b.areAllValuesNull(), equalTo(true));
        assertThat(b.isNull(0), equalTo(true));
        assertThat(b.getValueCount(0), equalTo(0));
    }

    protected static IntStream allValueOffsets(Block input) {
        return IntStream.range(0, input.getPositionCount()).flatMap(p -> {
            int start = input.getFirstValueIndex(p);
            int end = start + input.getValueCount(p);
            return IntStream.range(start, end);
        });
    }

    protected static Stream<BytesRef> allBytesRefs(Block input) {
        BytesRefBlock b = (BytesRefBlock) input;
        return allValueOffsets(b).mapToObj(i -> b.getBytesRef(i, new BytesRef()));
    }

    protected static Stream<Boolean> allBooleans(Block input) {
        BooleanBlock b = (BooleanBlock) input;
        return allValueOffsets(b).mapToObj(i -> b.getBoolean(i));
    }

    protected static DoubleStream allDoubles(Block input) {
        DoubleBlock b = (DoubleBlock) input;
        return allValueOffsets(b).mapToDouble(i -> b.getDouble(i));
    }

    protected static IntStream allInts(Block input) {
        IntBlock b = (IntBlock) input;
        return allValueOffsets(b).map(i -> b.getInt(i));
    }

    protected static LongStream allLongs(Block input) {
        LongBlock b = (LongBlock) input;
        return allValueOffsets(b).mapToLong(i -> b.getLong(i));
    }
}
