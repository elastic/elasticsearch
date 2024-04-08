/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockTestUtils;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TestBlockFactory;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.ForkingOperatorTestCase;
import org.elasticsearch.compute.operator.NullInsertingSourceOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PositionMergingSourceOperator;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class AggregatorFunctionTestCase extends ForkingOperatorTestCase {
    protected abstract AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels);

    protected final int aggregatorIntermediateBlockCount() {
        try (var agg = aggregatorFunction(List.of()).aggregator(driverContext())) {
            return agg.intermediateBlockCount();
        }
    }

    protected abstract String expectedDescriptionOfAggregator();

    protected abstract void assertSimpleOutput(List<Block> input, Block result);

    @Override
    protected Operator.OperatorFactory simpleWithMode(AggregatorMode mode) {
        List<Integer> channels = mode.isInputPartial() ? range(0, aggregatorIntermediateBlockCount()).boxed().toList() : List.of(0);
        return new AggregationOperator.AggregationOperatorFactory(List.of(aggregatorFunction(channels).aggregatorFactory(mode)), mode);
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

    public final void testIgnoresNulls() {
        int end = between(1_000, 100_000);
        List<Page> results = new ArrayList<>();
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(blockFactory, end));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());

        try (
            Driver d = new Driver(
                driverContext,
                new NullInsertingSourceOperator(new CannedSourceOperator(input.iterator()), blockFactory),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add),
                () -> {}
            )
        ) {
            runDriver(d);
        }
        assertSimpleOutput(origInput, results);
    }

    public final void testMultivalued() {
        int end = between(1_000, 100_000);
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        List<Page> input = CannedSourceOperator.collectPages(
            new PositionMergingSourceOperator(simpleInput(driverContext.blockFactory(), end), blockFactory)
        );
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        assertSimpleOutput(origInput, drive(simple().get(driverContext), input.iterator(), driverContext));
    }

    public final void testMultivaluedWithNulls() {
        int end = between(1_000, 100_000);
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        List<Page> input = CannedSourceOperator.collectPages(
            new NullInsertingSourceOperator(
                new PositionMergingSourceOperator(simpleInput(driverContext.blockFactory(), end), blockFactory),
                blockFactory
            )
        );
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        assertSimpleOutput(origInput, drive(simple().get(driverContext), input.iterator(), driverContext));
    }

    public final void testEmptyInput() {
        DriverContext driverContext = driverContext();
        List<Page> results = drive(simple().get(driverContext), List.<Page>of().iterator(), driverContext);

        assertThat(results, hasSize(1));
    }

    public final void testEmptyInputInitialFinal() {
        DriverContext driverContext = driverContext();
        var operators = List.of(
            simpleWithMode(AggregatorMode.INITIAL).get(driverContext),
            simpleWithMode(AggregatorMode.FINAL).get(driverContext)
        );
        List<Page> results = drive(operators, List.<Page>of().iterator(), driverContext);
        assertThat(results, hasSize(1));
    }

    public final void testEmptyInputInitialIntermediateFinal() {
        DriverContext driverContext = driverContext();
        var operators = List.of(
            simpleWithMode(AggregatorMode.INITIAL).get(driverContext),
            simpleWithMode(AggregatorMode.INTERMEDIATE).get(driverContext),
            simpleWithMode(AggregatorMode.FINAL).get(driverContext)
        );
        List<Page> results = drive(operators, List.<Page>of().iterator(), driverContext);

        assertThat(results, hasSize(1));
        assertOutputFromEmpty(results.get(0).getBlock(0));
    }

    // Returns an intermediate state that is equivalent to what the local execution planner will emit
    // if it determines that certain shards have no relevant data.
    final List<Page> nullIntermediateState(BlockFactory blockFactory) {
        try (var agg = aggregatorFunction(List.of()).aggregator(driverContext())) {
            var method = agg.getClass().getMethod("intermediateStateDesc");
            @SuppressWarnings("unchecked")
            List<IntermediateStateDesc> intermediateStateDescs = (List<IntermediateStateDesc>) method.invoke(null);
            List<Block> blocks = new ArrayList<>();
            for (var interSate : intermediateStateDescs) {
                try (var wrapper = BlockUtils.wrapperFor(blockFactory, interSate.type(), 1)) {
                    wrapper.accept(null);
                    blocks.add(wrapper.builder().build());
                }
            }
            return List.of(new Page(blocks.toArray(Block[]::new)));
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public final void testNullIntermediateFinal() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        List<Page> input = nullIntermediateState(blockFactory);
        var operators = List.of(
            simpleWithMode(AggregatorMode.INTERMEDIATE).get(driverContext),
            simpleWithMode(AggregatorMode.FINAL).get(driverContext)
        );
        List<Page> results = drive(operators, input.iterator(), driverContext);
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
