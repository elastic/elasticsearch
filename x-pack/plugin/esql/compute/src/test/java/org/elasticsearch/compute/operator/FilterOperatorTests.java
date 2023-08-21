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
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class FilterOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(int end) {
        return new TupleBlockSourceOperator(LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l, end - l)));
    }

    record SameLastDigit(int lhs, int rhs) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            LongVector lhsVector = page.<LongBlock>getBlock(0).asVector();
            LongVector rhsVector = page.<LongBlock>getBlock(1).asVector();
            BooleanVector.Builder result = BooleanVector.newVectorBuilder(page.getPositionCount());
            for (int p = 0; p < page.getPositionCount(); p++) {
                result.appendBoolean(lhsVector.getLong(p) % 10 == rhsVector.getLong(p) % 10);
            }
            return result.build().asBlock();
        }
    }

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        return new FilterOperator.FilterOperatorFactory(() -> new SameLastDigit(0, 1));
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "FilterOperator[evaluator=SameLastDigit[lhs=0, rhs=1]]";
    }

    @Override
    protected String expectedToStringOfSimple() {
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

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big arrays so can't break", false);
        return null;
    }
}
