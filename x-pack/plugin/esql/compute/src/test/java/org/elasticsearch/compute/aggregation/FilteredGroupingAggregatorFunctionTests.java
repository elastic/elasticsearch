/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.LongIntBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class FilteredGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    private final List<Exception> unclosed = Collections.synchronizedList(new ArrayList<>());

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new FilteredAggregatorFunctionSupplier(
            new SumIntAggregatorFunctionSupplier(inputChannels),
            new AnyGreaterThanFactory(unclosed, inputChannels)
        );
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "Filtered[next=sum of ints, filter=any > 0]";
    }

    @Override
    protected String expectedToStringOfSimpleAggregator() {
        return "FilteredGroupingAggregatorFunction[next=SumIntGroupingAggregatorFunction[channels=[1]], filter=any > 0]";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        long sum = 0;
        for (Page page : input) {
            LongBlock groups = page.getBlock(0);
            IntBlock ints = page.getBlock(1);
            for (int p = 0; p < ints.getPositionCount(); p++) {
                /*
                 * Perform the sum on the values *only* if:
                 * 1. Any of the values is > 0 to line up with the condition
                 * 2. Any of the groups matches the group we're asserting
                 */
                int start = ints.getFirstValueIndex(p);
                int end = start + ints.getValueCount(p);
                boolean selected = false;
                for (int i = start; i < end; i++) {
                    selected |= ints.getInt(i) > 0;
                }
                if (selected == false) {
                    continue;
                }
                selected = false;
                if (group == null) {
                    selected = groups.isNull(p);
                } else {
                    start = groups.getFirstValueIndex(p);
                    end = start + groups.getValueCount(p);
                    for (int i = start; i < end; i++) {
                        selected |= groups.getLong(i) == group;
                    }
                }
                if (selected == false) {
                    continue;
                }

                start = ints.getFirstValueIndex(p);
                end = start + ints.getValueCount(p);
                for (int i = start; i < end; i++) {
                    sum += ints.getInt(i);
                }
            }
        }
        assertThat(((LongBlock) result).getLong(position), equalTo(sum));
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        int max = between(1, Integer.MAX_VALUE / size / 5);
        return new LongIntBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), between(-max, max)))
        );
    }

    /**
     * Tests {@link GroupingAggregator#addIntermediateRow} by building results using the traditional
     * add mechanism and using {@link GroupingAggregator#addIntermediateRow} then asserting that they
     * produce the same output.
     */
    public void testAddIntermediateRowInput() {
        DriverContext ctx = driverContext();
        AggregatorFunctionSupplier supplier = aggregatorFunction(channels(AggregatorMode.SINGLE));
        Block[] results = new Block[2];
        try (
            GroupingAggregatorFunction main = supplier.groupingAggregator(ctx);
            GroupingAggregatorFunction leaf = supplier.groupingAggregator(ctx);
            SourceOperator source = simpleInput(ctx.blockFactory(), 10);
        ) {
            Page p;
            while ((p = source.getOutput()) != null) {
                try (
                    IntVector group = ctx.blockFactory().newConstantIntVector(0, p.getPositionCount());
                    GroupingAggregatorFunction.AddInput addInput = leaf.prepareProcessPage(null, p)
                ) {
                    addInput.add(0, group);
                } finally {
                    p.releaseBlocks();
                }
            }
            main.addIntermediateRowInput(0, leaf, 0);
            try (IntVector selected = ctx.blockFactory().newConstantIntVector(0, 1)) {
                main.evaluateFinal(results, 0, selected, ctx);
                leaf.evaluateFinal(results, 1, selected, ctx);
            }
            assertThat(results[0], equalTo(results[1]));
        } finally {
            Releasables.close(results);
        }
    }

    @After
    public void checkUnclosed() {
        for (Exception tracker : unclosed) {
            logger.error("unclosed", tracker);
        }
        assertThat(unclosed, empty());
    }

    /**
     * This checks if *any* of the integers are > 0. If so we push the group to
     * the aggregation.
     */
    record AnyGreaterThanFactory(List<Exception> unclosed, List<Integer> inputChannels)
        implements
            EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            Exception tracker = new Exception(Integer.toString(unclosed.size()));
            unclosed.add(tracker);
            return new AnyGreaterThan(context.blockFactory(), unclosed, tracker, inputChannels);
        }

        @Override
        public String toString() {
            return "any > 0";
        }
    }

    private record AnyGreaterThan(BlockFactory blockFactory, List<Exception> unclosed, Exception tracker, List<Integer> inputChannels)
        implements
            EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            IntBlock ints = page.getBlock(inputChannels.get(0));
            try (BooleanVector.FixedBuilder result = blockFactory.newBooleanVectorFixedBuilder(ints.getPositionCount())) {
                position: for (int p = 0; p < ints.getPositionCount(); p++) {
                    int start = ints.getFirstValueIndex(p);
                    int end = start + ints.getValueCount(p);
                    for (int i = start; i < end; i++) {
                        if (ints.getInt(i) > 0) {
                            result.appendBoolean(p, true);
                            continue position;
                        }
                    }
                    result.appendBoolean(p, false);
                }
                return result.build().asBlock();
            }
        }

        @Override
        public void close() {
            if (unclosed.remove(tracker) == false) {
                throw new IllegalStateException("close failure!");
            }
        }

        @Override
        public String toString() {
            return "any > 0";
        }
    }
}
