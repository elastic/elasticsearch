/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.MultiEvalOperator.MultiEvalOperatorFactory;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class MultiEvalOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new TupleLongLongBlockSourceOperator(blockFactory, LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l, end - l)));
    }

    /**
     * Sums the two input channels for every position. The result of {@link #simpleInput} makes every row sum to
     * {@code end}, so each appended column holds the same constant value.
     */
    record Addition(DriverContext driverContext, int lhs, int rhs) implements ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            LongVector lhsVector = page.<LongBlock>getBlock(lhs).asVector();
            LongVector rhsVector = page.<LongBlock>getBlock(rhs).asVector();
            try (LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(page.getPositionCount())) {
                for (int p = 0; p < page.getPositionCount(); p++) {
                    result.appendLong(lhsVector.getLong(p) + rhsVector.getLong(p));
                }
                return result.build().asBlock();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return 1;
        }

        @Override
        public String toString() {
            return "Addition[lhs=" + lhs + ", rhs=" + rhs + ']';
        }

        @Override
        public void close() {}
    }

    private static ExpressionEvaluator.Factory additionFactory(int lhs, int rhs) {
        return new ExpressionEvaluator.Factory() {
            @Override
            public ExpressionEvaluator get(DriverContext context) {
                return new Addition(context, lhs, rhs);
            }

            @Override
            public String toString() {
                return "Addition[lhs=" + lhs + ", rhs=" + rhs + ']';
            }
        };
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new MultiEvalOperatorFactory(List.of(additionFactory(0, 1), additionFactory(0, 1)));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("MultiEvalOperator[evaluators=[Addition[lhs=0, rhs=1], Addition[lhs=0, rhs=1]]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        final int positions = input.stream().map(page -> page.<Block>getBlock(0)).mapToInt(Block::getPositionCount).sum();
        final int expectedValue = positions;
        // Both batched evaluators append their own column: channels 2 and 3 both hold the constant sum.
        for (var page : results) {
            assertThat(page.getBlockCount(), equalTo(4));
            LongBlock first = page.getBlock(2);
            LongBlock second = page.getBlock(3);
            IntStream.range(0, first.getPositionCount()).forEach(pos -> {
                assertEquals(expectedValue, first.getLong(pos));
                assertEquals(expectedValue, second.getLong(pos));
            });
        }
    }

    /**
     * An evaluator that just loads a channel from the input page, incrementing its reference count. Two of them in a
     * batch verify that each appended block is owned by the output page and released exactly once.
     */
    record LoadFromPage(int channel) implements ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            Block block = page.getBlock(channel);
            block.incRef();
            return block;
        }

        @Override
        public long baseRamBytesUsed() {
            return 2;
        }

        @Override
        public void close() {}
    }

    public void testBatchLoadsEachColumnIndependently() {
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(simpleInput(runner.blockFactory(), 10));
        List<Page> results = runner.run(
            new MultiEvalOperatorFactory(List.of(dvrCtx -> new LoadFromPage(0), dvrCtx -> new LoadFromPage(1)))
        );
        for (var page : results) {
            assertThat(page.getBlockCount(), equalTo(4));
            LongBlock loadedFirst = page.getBlock(2);
            LongBlock loadedSecond = page.getBlock(3);
            for (int pos = 0; pos < loadedFirst.getPositionCount(); pos++) {
                // channel 0 is l, channel 1 is end - l; loading both back reproduces the inputs
                assertEquals(loadedFirst.getLong(pos) + loadedSecond.getLong(pos), 10);
            }
        }
        results.forEach(Page::releaseBlocks);
        assertThat(runner.context().breaker().getUsed(), equalTo(0L));
    }
}
