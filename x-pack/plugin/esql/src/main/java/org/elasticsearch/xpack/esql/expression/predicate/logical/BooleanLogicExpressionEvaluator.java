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
            // Empty array vectors are vacuously allTrue() and allFalse(); skip the short-cut.
            if (lhs.getPositionCount() != 0) {
                // Absorb both sides before identity. true AND (all-false vector) must
                // become a constant, not an incRef of the n-sized array.
                if (lhsVector != null) {
                    Block absorb = tryAbsorb((BooleanVector) lhsVector, rhs);
                    if (absorb != null) {
                        return absorb;
                    }
                }
                if (rhsVector != null) {
                    Block absorb = tryAbsorb((BooleanVector) rhsVector, lhs);
                    if (absorb != null) {
                        return absorb;
                    }
                }
                if (lhsVector != null) {
                    Block identity = tryIdentity((BooleanVector) lhsVector, rhs);
                    if (identity != null) {
                        return identity;
                    }
                }
                if (rhsVector != null) {
                    Block identity = tryIdentity((BooleanVector) rhsVector, lhs);
                    if (identity != null) {
                        return identity;
                    }
                }
            }
            if (lhsVector != null && rhsVector != null) {
                return eval((BooleanVector) lhsVector, (BooleanVector) rhsVector);
            }
            return eval(lhs, rhs);
        }
    }

    /**
     * Absorbing short-cut: {@code false AND *} / {@code true OR *}. MV-guard is here
     * so {@code false AND MV} / {@code true OR MV} stay null.
     */
    private Block tryAbsorb(BooleanVector vector, Block other) {
        if (other.mayHaveMultivaluedFields()) {
            return null;
        }
        return switch (bl.function()) {
            case AND -> vector.allFalse() ? vector.blockFactory().newConstantBooleanBlockWith(false, vector.getPositionCount()) : null;
            case OR -> vector.allTrue() ? vector.blockFactory().newConstantBooleanBlockWith(true, vector.getPositionCount()) : null;
        };
    }

    /**
     * Identity short-cut: {@code true AND x} / {@code false OR x} returns the eval
     * {@link Block} {@code other}, never {@code vector.asBlock()} — a second wrapper
     * would double-close. Same MV-guard as {@link #tryAbsorb}.
     */
    private Block tryIdentity(BooleanVector vector, Block other) {
        if (other.mayHaveMultivaluedFields()) {
            return null;
        }
        return switch (bl.function()) {
            case AND -> {
                if (vector.allTrue()) {
                    other.incRef();
                    yield other;
                }
                yield null;
            }
            case OR -> {
                if (vector.allFalse()) {
                    other.incRef();
                    yield other;
                }
                yield null;
            }
        };
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    /**
     * Eval blocks, handling {@code null} and multi-values. Takes {@link Block}
     * because {@code ConstantNullBlock} implements {@link BooleanBlock} but
     * still has no boolean values to read; check {@code isNull} first.
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
            return switch (bl.function()) {
                case AND -> {
                    andVectors(lhs, rhs, result);
                    yield result.build().asBlock();
                }
                case OR -> {
                    orVectors(lhs, rhs, result);
                    yield result.build().asBlock();
                }
            };
        }
    }

    // 2VL only: vectors have no nulls and no multi-values. Do not reuse on the block path.
    private static void andVectors(BooleanVector lhs, BooleanVector rhs, BooleanVector.FixedBuilder result) {
        int positionCount = lhs.getPositionCount();
        for (int p = 0; p < positionCount; p++) {
            result.appendBoolean(p, lhs.getBoolean(p) & rhs.getBoolean(p));
        }
    }

    // 2VL only: vectors have no nulls and no multi-values. Do not reuse on the block path.
    private static void orVectors(BooleanVector lhs, BooleanVector rhs, BooleanVector.FixedBuilder result) {
        int positionCount = lhs.getPositionCount();
        for (int p = 0; p < positionCount; p++) {
            result.appendBoolean(p, lhs.getBoolean(p) | rhs.getBoolean(p));
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
