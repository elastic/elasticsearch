/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
* Evaluator for the <a href="https://en.wikipedia.org/wiki/Three-valued_logic">three-valued boolean expressions</a>.
* We can't generate these with the {@link Evaluator} annotation because that
* always implements viral null. And three-valued boolean expressions don't.
* {@code false AND null} is {@code false} and {@code true OR null} is {@code true}.
*/
public record BooleanLogicExpressionEvaluator(BinaryLogic bl, ExpressionEvaluator leftEval, ExpressionEvaluator rightEval)
    implements
        ExpressionEvaluator {
    public record Factory(BinaryLogic bl, ExpressionEvaluator.Factory leftEval, ExpressionEvaluator.Factory rightEval)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext driverContext) {
            return new BooleanLogicExpressionEvaluator(bl, leftEval.get(driverContext), rightEval.get(driverContext));
        }

        @Override
        public String toString() {
            return "BooleanLogicExpressionEvaluator[bl=" + bl + ", leftEval=" + leftEval + ", rightEval=" + rightEval + ']';
        }
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BooleanLogicExpressionEvaluator.class);

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

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED;
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
                Boolean v = bl.function()
                    .apply(
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
                result.appendBoolean(p, bl.function().apply(lhs.getBoolean(p), rhs.getBoolean(p)));
            }
            return result.build().asBlock();
        }
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(leftEval, rightEval);
    }

    @Override
    public String toString() {
        return "BooleanLogicExpressionEvaluator[bl=" + bl + ", leftEval=" + leftEval + ", rightEval=" + rightEval + ']';
    }
}
