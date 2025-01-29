/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

import java.util.Objects;

/**
 * Expression evaluator for boolean logic operations. To be used when scoring is not used
 */
public class BooleanLogicExpressionEvaluator implements EvalOperator.ExpressionEvaluator {
    private final BinaryLogicOperation bl;
    private final EvalOperator.ExpressionEvaluator leftEval;
    private final EvalOperator.ExpressionEvaluator rightEval;

    public BooleanLogicExpressionEvaluator(
        BinaryLogicOperation bl,
        EvalOperator.ExpressionEvaluator leftEval,
        EvalOperator.ExpressionEvaluator rightEval
    ) {
        this.bl = bl;
        this.leftEval = leftEval;
        this.rightEval = rightEval;
    }

    @Override
    public Block eval(Page page) {
        try (Block lhs = leftEval.eval(page); Block rhs = rightEval.eval(page)) {
            Vector lhsVector = lhs.asVector();
            Vector rhsVector = rhs.asVector();
            if (lhsVector != null && rhsVector != null) {
                return eval((BooleanVector) lhsVector, (BooleanVector) rhsVector);
            }
            return eval(lhs, rhs);
        }
    }

    /**
     * Eval blocks, handling {@code null}. This takes {@link Block} instead of
     * {@link BooleanBlock} because blocks that <strong>only</strong> contain
     * {@code null} can't be cast to {@link BooleanBlock}. So we check for
     * {@code null} first and don't cast at all if the value is {@code null}.
     */
    private Block eval(Block lhs, Block rhs) {
        int positionCount = lhs.getPositionCount();
        try (BooleanBlock.Builder result = lhs.blockFactory().newBooleanBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (lhs.getValueCount(p) > 1) {
                    result.appendNull();
                    continue;
                }
                if (rhs.getValueCount(p) > 1) {
                    result.appendNull();
                    continue;
                }
                Boolean v = bl.apply(
                    lhs.isNull(p) ? null : ((BooleanBlock) lhs).getBoolean(lhs.getFirstValueIndex(p)),
                    rhs.isNull(p) ? null : ((BooleanBlock) rhs).getBoolean(rhs.getFirstValueIndex(p))
                );
                if (v == null) {
                    result.appendNull();
                    continue;
                }
                result.appendBoolean(v);
            }
            return result.build();
        }
    }

    private Block eval(BooleanVector lhs, BooleanVector rhs) {
        int positionCount = lhs.getPositionCount();
        try (var result = lhs.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                result.appendBoolean(p, bl.apply(lhs.getBoolean(p), rhs.getBoolean(p)));
            }
            return result.build().asBlock();
        }
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(leftEval, rightEval);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (BooleanLogicExpressionEvaluator) obj;
        return Objects.equals(this.bl, that.bl)
            && Objects.equals(this.leftEval, that.leftEval)
            && Objects.equals(this.rightEval, that.rightEval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bl, leftEval, rightEval);
    }

    @Override
    public String toString() {
        return "BooleanLogicExpressionEvaluator[" + "bl=" + bl + ", " + "leftEval=" + leftEval + ", " + "rightEval=" + rightEval + ']';
    }

    public static class BooleanLogicEvaluatorFactory implements Factory {

        private final BinaryLogicOperation bl;
        private final EvalOperator.ExpressionEvaluator leftEval;
        private final EvalOperator.ExpressionEvaluator rightEval;

        BooleanLogicEvaluatorFactory(
            BinaryLogicOperation bl,
            EvalOperator.ExpressionEvaluator leftEval,
            EvalOperator.ExpressionEvaluator rightEval
        ) {
            this.bl = bl;
            this.leftEval = leftEval;
            this.rightEval = rightEval;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new BooleanLogicExpressionEvaluator(bl, leftEval, rightEval);
        }
    }

}
