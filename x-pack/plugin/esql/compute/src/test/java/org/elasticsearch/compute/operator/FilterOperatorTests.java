/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class FilterOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new TupleBlockSourceOperator(blockFactory, LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l, end - l)));
    }

    record SameLastDigit(DriverContext context, int lhs, int rhs) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            LongVector lhsVector = page.<LongBlock>getBlock(0).asVector();
            LongVector rhsVector = page.<LongBlock>getBlock(1).asVector();
            BooleanVector.FixedBuilder result = context.blockFactory().newBooleanVectorFixedBuilder(page.getPositionCount());
            for (int p = 0; p < page.getPositionCount(); p++) {
                result.appendBoolean(lhsVector.getLong(p) % 10 == rhsVector.getLong(p) % 10);
            }
            return result.build().asBlock();
        }

        @Override
        public String toString() {
            return "SameLastDigit[lhs=" + lhs + ", rhs=" + rhs + ']';
        }

        @Override
        public void close() {}
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new FilterOperator.FilterOperatorFactory(new EvalOperator.ExpressionEvaluator.Factory() {

            @Override
            public EvalOperator.ExpressionEvaluator get(DriverContext context) {
                return new SameLastDigit(context, 0, 1);
            }

            @Override
            public String toString() {
                return "SameLastDigit[lhs=0, rhs=1]";
            }
        });
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("FilterOperator[evaluator=SameLastDigit[lhs=0, rhs=1]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int expectedCount = 0;
        for (var page : input) {
            LongVector lhs = page.<LongBlock>getBlock(0).asVector();
            LongVector rhs = page.<LongBlock>getBlock(1).asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (lhs.getLong(p) % 10 == rhs.getLong(p) % 10) {
                    expectedCount++;
                }
            }
        }
        int actualCount = 0;
        for (var page : results) {
            LongVector lhs = page.<LongBlock>getBlock(0).asVector();
            LongVector rhs = page.<LongBlock>getBlock(1).asVector();
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(lhs.getLong(p) % 10, equalTo(rhs.getLong(p) % 10));
                actualCount++;
            }
        }
        assertThat(actualCount, equalTo(expectedCount));
    }

    public void testNoResults() {
        assertSimple(driverContext(), 3);
    }

    public void testReadFromBlock() {
        DriverContext context = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(
            new SequenceBooleanBlockSourceOperator(context.blockFactory(), List.of(true, false, true, false))
        );
        List<Page> results = drive(
            new FilterOperator.FilterOperatorFactory(dvrCtx -> new EvalOperatorTests.LoadFromPage(0)).get(context),
            input.iterator(),
            context
        );
        List<Boolean> found = new ArrayList<>();
        for (var page : results) {
            BooleanVector lb = page.<BooleanBlock>getBlock(0).asVector();
            IntStream.range(0, lb.getPositionCount()).forEach(pos -> found.add(lb.getBoolean(pos)));
        }
        assertThat(found, equalTo(List.of(true, true)));
        results.forEach(Page::releaseBlocks);
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }
}
