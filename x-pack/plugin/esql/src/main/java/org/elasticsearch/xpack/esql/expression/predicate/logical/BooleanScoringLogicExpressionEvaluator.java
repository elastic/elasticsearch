/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

import java.util.Objects;

public class BooleanScoringLogicExpressionEvaluator implements EvalOperator.ExpressionEvaluator {
    private final BinaryScoringLogicOperation bsl;
    private final EvalOperator.ExpressionEvaluator leftEval;
    private final EvalOperator.ExpressionEvaluator rightEval;

    public BooleanScoringLogicExpressionEvaluator(
        BinaryScoringLogicOperation bsl,
        EvalOperator.ExpressionEvaluator leftEval,
        EvalOperator.ExpressionEvaluator rightEval
    ) {
        this.bsl = bsl;
        this.leftEval = leftEval;
        this.rightEval = rightEval;
    }

    @Override
    public Block eval(Page page) {
        try (Block lhs = leftEval.eval(page); Block rhs = rightEval.eval(page)) {
            Vector lhsVector = lhs.asVector();
            Vector rhsVector = rhs.asVector();
            if (lhsVector != null && rhsVector != null) {
                return eval((DoubleVector) lhsVector, (DoubleVector) rhsVector);
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
        try (DoubleBlock.Builder result = lhs.blockFactory().newDoubleBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (lhs.getValueCount(p) > 1) {
                    result.appendNull();
                    continue;
                }
                if (rhs.getValueCount(p) > 1) {
                    result.appendNull();
                    continue;
                }
                Double v = bsl.apply(
                    lhs.isNull(p) ? null : ((DoubleBlock) lhs).getDouble(lhs.getFirstValueIndex(p)),
                    rhs.isNull(p) ? null : ((DoubleBlock) rhs).getDouble(rhs.getFirstValueIndex(p))
                );
                if (v == null) {
                    result.appendNull();
                    continue;
                }
                result.appendDouble(v);
            }
            return result.build();
        }
    }

    private Block eval(DoubleVector lhs, DoubleVector rhs) {
        int positionCount = lhs.getPositionCount();
        try (var result = lhs.blockFactory().newDoubleVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                result.appendDouble(p, bsl.apply(lhs.getDouble(p), rhs.getDouble(p)));
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
        var that = (BooleanScoringLogicExpressionEvaluator) obj;
        return Objects.equals(this.bsl, that.bsl)
            && Objects.equals(this.leftEval, that.leftEval)
            && Objects.equals(this.rightEval, that.rightEval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bsl, leftEval, rightEval);
    }

    @Override
    public String toString() {
        return "BooleanScoringLogicExpressionEvaluator["
            + "bl="
            + bsl
            + ", "
            + "leftEval="
            + leftEval
            + ", "
            + "rightEval="
            + rightEval
            + ']';
    }

    public static class BooleanScoringLogicEvaluatorFactory implements Factory {

        private final BinaryScoringLogicOperation bsl;
        private final EvalOperator.ExpressionEvaluator leftEval;
        private final EvalOperator.ExpressionEvaluator rightEval;

        BooleanScoringLogicEvaluatorFactory(
            BinaryScoringLogicOperation bsl,
            EvalOperator.ExpressionEvaluator leftEval,
            EvalOperator.ExpressionEvaluator rightEval
        ) {
            this.bsl = bsl;
            this.leftEval = leftEval;
            this.rightEval = rightEval;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new BooleanScoringLogicExpressionEvaluator(bsl, leftEval, rightEval);
        }
    }
}
