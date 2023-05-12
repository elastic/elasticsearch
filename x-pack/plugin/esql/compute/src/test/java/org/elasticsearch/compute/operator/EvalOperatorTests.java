/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class EvalOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(int end) {
        return new TupleBlockSourceOperator(LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l, end - l)));
    }

    record Addition(int lhs, int rhs) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            LongVector lhsVector = page.<LongBlock>getBlock(0).asVector();
            LongVector rhsVector = page.<LongBlock>getBlock(1).asVector();
            LongVector.Builder result = LongVector.newVectorBuilder(page.getPositionCount());
            for (int p = 0; p < page.getPositionCount(); p++) {
                result.appendLong(lhsVector.getLong(p) + rhsVector.getLong(p));
            }
            return result.build().asBlock();
        }
    }

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        return new EvalOperator.EvalOperatorFactory(() -> new Addition(0, 1));
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "EvalOperator[evaluator=Addition[lhs=0, rhs=1]]";
    }

    @Override
    protected String expectedToStringOfSimple() {
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

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big arrays so can't break", false);
        return null;
    }
}
