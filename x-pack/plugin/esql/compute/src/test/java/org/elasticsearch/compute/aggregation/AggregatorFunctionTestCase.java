/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ConstantBooleanExpressionEvaluator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AddGarbageRowsSourceOperator;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.ForkingOperatorTestCase;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PositionMergingSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.NullInsertingSourceOperator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class AggregatorFunctionTestCase extends ForkingOperatorTestCase {
    protected abstract AggregatorFunctionSupplier aggregatorFunction();

    protected final int aggregatorIntermediateBlockCount() {
        try (var agg = aggregatorFunction().aggregator(driverContext(), List.of())) {
            return agg.intermediateBlockCount();
        }
    }

    protected abstract String expectedDescriptionOfAggregator();

    /**
     * Assert that the result is correct given the input.
     * @param input the input pages build by {@link #simpleInput}
     * @param result the result of running {@link #aggregatorFunction()}
     */
    protected abstract void assertSimpleOutput(List<Page> input, Block result);

    @Override
    protected Operator.OperatorFactory simpleWithMode(SimpleOptions options, AggregatorMode mode) {
        return simpleWithMode(mode, Function.identity());
    }

    private Operator.OperatorFactory simpleWithMode(
        AggregatorMode mode,
        Function<AggregatorFunctionSupplier, AggregatorFunctionSupplier> wrap
    ) {
        List<Integer> channels = mode.isInputPartial()
            ? range(0, aggregatorIntermediateBlockCount()).boxed().toList()
            : IntStream.range(0, inputCount()).boxed().toList();
        AggregatorFunctionSupplier supplier = aggregatorFunction();
        Aggregator.Factory factory = wrap.apply(supplier).aggregatorFactory(mode, channels);
        return new AggregationOperator.AggregationOperatorFactory(List.of(factory), mode);
    }

    protected int inputCount() {
        return 1;
    }

    @Override
    protected final Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("AggregationOperator[mode = SINGLE, aggs = " + expectedDescriptionOfAggregator() + "]");
    }

    @Override
    protected final Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "AggregationOperator[aggregators=[Aggregator[aggregatorFunction=" + expectedToStringOfSimpleAggregator() + ", mode=SINGLE]]]"
        );
    }

    protected String expectedToStringOfSimpleAggregator() {
        String type = getClass().getSimpleName().replace("Tests", "");
        return type + "[channels=" + IntStream.range(0, inputCount()).boxed().toList() + "]";
    }

    @Override
    protected final void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(1));
        assertThat(results.get(0).getPositionCount(), equalTo(1));

        Block result = results.get(0).getBlock(0);
        assertSimpleOutput(input, result);
    }

    /**
     * Defines how large datasets are generated, should be overridden for complex types to reduce test runtime.
     *
     * @return the maximum number of rows to generate for tests
     */
    protected int maximumTestRowCount() {
        return 100_000;
    }

    public final void testIgnoresNulls() {
        int end = between(1_000, maximumTestRowCount());
        List<Page> results = new ArrayList<>();
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(blockFactory, end));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());

        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new NullInsertingSourceOperator(new CannedSourceOperator(input.iterator()), blockFactory),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(d);
        }
        assertSimpleOutput(origInput, results);
    }

    protected boolean supportsMultiValues() {
        return true;
    }

    public final void testMultivalued() {
        assumeTrue("Multivalues support is required for the tested type", supportsMultiValues());
        int end = between(1_000, maximumTestRowCount());
        var runner = new TestDriverRunner().builder(driverContext()).collectDeepCopy();
        runner.input(new PositionMergingSourceOperator(simpleInput(runner.blockFactory(), end), runner.blockFactory()));
        assertSimpleOutput(runner.deepCopy(), runner.run(simple()));
    }

    public final void testMultivaluedWithNulls() {
        assumeTrue("Multivalues support is required for the tested type", supportsMultiValues());
        int end = between(1_000, maximumTestRowCount());
        var runner = new TestDriverRunner().builder(driverContext()).collectDeepCopy().insertNulls();
        runner.input(new PositionMergingSourceOperator(simpleInput(runner.blockFactory(), end), runner.blockFactory()));
        assertSimpleOutput(runner.deepCopy(), runner.run(simple()));
    }

    public final void testEmptyInput() {
        List<Page> results = new TestDriverRunner().builder(driverContext()).input(List.<Page>of()).run(simple());
        assertThat(results, hasSize(1));
        assertOutputFromEmpty(results.get(0).getBlock(0));
    }

    public final void testEmptyInputInitialFinal() {
        var runner = new TestDriverRunner().builder(driverContext()).input(List.<Page>of());
        List<Page> results = runner.run(simpleWithMode(AggregatorMode.INITIAL), simpleWithMode(AggregatorMode.FINAL));
        assertThat(results, hasSize(1));
    }

    public final void testEmptyInputInitialIntermediateFinal() {
        var runner = new TestDriverRunner().builder(driverContext()).input(List.<Page>of());
        List<Page> results = runner.run(
            simpleWithMode(AggregatorMode.INITIAL),
            simpleWithMode(AggregatorMode.INTERMEDIATE),
            simpleWithMode(AggregatorMode.FINAL)
        );
        assertThat(results, hasSize(1));
        assertOutputFromEmpty(results.get(0).getBlock(0));
    }

    public void testAllFiltered() {
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(simpleInput(runner.blockFactory(), 10));
        List<Page> results = runner.run(
            simpleWithMode(
                AggregatorMode.SINGLE,
                agg -> new FilteredAggregatorFunctionSupplier(agg, ConstantBooleanExpressionEvaluator.factory(false))
            )
        );
        assertThat(results, hasSize(1));
        assertOutputFromEmpty(results.get(0).getBlock(0));
    }

    public void testNoneFiltered() {
        var runner = new TestDriverRunner().builder(driverContext()).collectDeepCopy();
        runner.input(simpleInput(runner.blockFactory(), 10));
        List<Page> results = runner.run(
            simpleWithMode(
                AggregatorMode.SINGLE,
                agg -> new FilteredAggregatorFunctionSupplier(agg, ConstantBooleanExpressionEvaluator.factory(true))
            )
        );
        assertThat(results, hasSize(1));
        assertSimpleOutput(runner.deepCopy(), results);
    }

    public void testSomeFiltered() {
        var runner = new TestDriverRunner().builder(driverContext()).collectDeepCopy();
        // Build the test data
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(runner.blockFactory(), 10));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        // Sprinkle garbage into it
        SourceOperator withGarbage = new AddGarbageRowsSourceOperator(new CannedSourceOperator(input.iterator()));
        List<Page> results = runner.input(withGarbage)
            .run(
                simpleWithMode(
                    AggregatorMode.SINGLE,
                    agg -> new FilteredAggregatorFunctionSupplier(agg, AddGarbageRowsSourceOperator.filterFactory())
                )
            );
        assertThat(results, hasSize(1));
        assertSimpleOutput(origInput, results);
    }

    // Returns an intermediate state that is equivalent to what the local execution planner will emit
    // if it determines that certain shards have no relevant data.
    List<Page> nullIntermediateState(BlockFactory blockFactory) {
        try (var agg = aggregatorFunction().aggregator(driverContext(), List.of())) {
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
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(nullIntermediateState(runner.blockFactory()));
        List<Page> results = runner.run(simpleWithMode(AggregatorMode.INTERMEDIATE), simpleWithMode(AggregatorMode.FINAL));
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

    protected static Stream<Float> allFloats(Block input) {
        FloatBlock b = (FloatBlock) input;
        return allValueOffsets(b).mapToObj(b::getFloat);
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
