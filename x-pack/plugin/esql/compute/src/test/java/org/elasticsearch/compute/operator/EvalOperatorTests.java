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
import org.elasticsearch.compute.operator.EvalOperator.EvalOperatorFactory;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class EvalOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new TupleBlockSourceOperator(blockFactory, LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l, end - l)));
    }

    record Addition(DriverContext driverContext, int lhs, int rhs) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            LongVector lhsVector = page.<LongBlock>getBlock(0).asVector();
            LongVector rhsVector = page.<LongBlock>getBlock(1).asVector();
            try (LongVector.FixedBuilder result = driverContext.blockFactory().newLongVectorFixedBuilder(page.getPositionCount())) {
                for (int p = 0; p < page.getPositionCount(); p++) {
                    result.appendLong(lhsVector.getLong(p) + rhsVector.getLong(p));
                }
                return result.build().asBlock();
            }
        }

        @Override
        public String toString() {
            return "Addition[lhs=" + lhs + ", rhs=" + rhs + ']';
        }

        @Override
        public void close() {}
    }

    record LoadFromPage(int channel) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            Block block = page.getBlock(channel);
            block.incRef();
            return block;
        }

        @Override
        public void close() {}
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new EvalOperator.EvalOperatorFactory(new EvalOperator.ExpressionEvaluator.Factory() {
            @Override
            public EvalOperator.ExpressionEvaluator get(DriverContext context) {
                return new Addition(context, 0, 1);
            }

            @Override
            public String toString() {
                return "Addition[lhs=0, rhs=1]";
            }
        });
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("EvalOperator[evaluator=Addition[lhs=0, rhs=1]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        final int positions = input.stream().map(page -> page.<Block>getBlock(0)).mapToInt(Block::getPositionCount).sum();
        final int expectedValue = positions;
        final int resultChannel = 2;
        for (var page : results) {
            LongBlock lb = page.getBlock(resultChannel);
            IntStream.range(0, lb.getPositionCount()).forEach(pos -> assertEquals(expectedValue, lb.getLong(pos)));
        }
    }

    public void testReadFromBlock() {
        DriverContext context = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(context.blockFactory(), 10));
        List<Page> results = drive(new EvalOperatorFactory(dvrCtx -> new LoadFromPage(0)).get(context), input.iterator(), context);
        Set<Long> found = new TreeSet<>();
        for (var page : results) {
            LongBlock lb = page.getBlock(2);
            IntStream.range(0, lb.getPositionCount()).forEach(pos -> found.add(lb.getLong(pos)));
        }
        assertThat(found, equalTo(LongStream.range(0, 10).mapToObj(Long::valueOf).collect(Collectors.toSet())));
        results.forEach(Page::releaseBlocks);
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }
}
