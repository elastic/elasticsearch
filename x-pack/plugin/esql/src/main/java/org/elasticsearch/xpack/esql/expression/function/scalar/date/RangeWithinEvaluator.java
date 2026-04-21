/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Evaluator for RANGE_WITHIN(value, range) -> boolean.
 * Supports (date, date_range) and (date_range, date_range) with search WITHIN/CONTAINS semantics (range is container).
 *
 * TODO: Move type branching out of the per-position hot path. Other functions use one evaluator per type
 * combination (via @Evaluator annotations), so the branch is at factory/evaluator-creation time, not per row.
 * Ideally add date_range support to @Evaluator annotations first to avoid scaffolding; then refactor to
 * separate evaluators for (date, date_range) vs (date_range, date_range). Fine to merge as-is; prioritize
 * this follow-up so we don't forget.
 */
public class RangeWithinEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RangeWithinEvaluator.class);

    @SuppressWarnings("unused")  // reserved for error reporting
    private final Source source;
    private final DataType leftType;
    private final DataType rightType;
    private final ExpressionEvaluator leftEvaluator;
    private final ExpressionEvaluator rightEvaluator;
    private final DriverContext driverContext;

    public RangeWithinEvaluator(
        Source source,
        DataType leftType,
        DataType rightType,
        ExpressionEvaluator leftEvaluator,
        ExpressionEvaluator rightEvaluator,
        DriverContext driverContext
    ) {
        this.source = source;
        this.leftType = leftType;
        this.rightType = rightType;
        this.leftEvaluator = leftEvaluator;
        this.rightEvaluator = rightEvaluator;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(org.elasticsearch.compute.data.Page page) {
        try (Block leftBlock = leftEvaluator.eval(page); Block rightBlock = rightEvaluator.eval(page)) {
            return eval(leftBlock, rightBlock);
        }
    }

    private Block eval(Block leftBlock, Block rightBlock) {
        int positionCount = leftBlock.getPositionCount();
        try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (leftBlock.isNull(p) || rightBlock.isNull(p)) {
                    result.appendNull();
                    continue;
                }
                boolean contains = evalPosition(leftBlock, rightBlock, p);
                result.appendBoolean(contains);
            }
            return result.build();
        }
    }

    /**
     * Returns true if any (left value, right value) pair satisfies "left within right" (right is the container).
     * Supports multivalued date_range and date. Only (date, date_range) and (date_range, date_range).
     */
    private boolean evalPosition(Block leftBlock, Block rightBlock, int p) {
        if (leftType == DataType.DATE_RANGE && rightType == DataType.DATE_RANGE) {
            LongRangeBlock leftRange = (LongRangeBlock) leftBlock;
            LongRangeBlock rightRange = (LongRangeBlock) rightBlock;
            int leftFirst = leftRange.getFirstValueIndex(p);
            int leftCount = leftRange.getValueCount(p);
            int rightFirst = rightRange.getFirstValueIndex(p);
            int rightCount = rightRange.getValueCount(p);
            LongBlock leftFrom = leftRange.getFromBlock();
            LongBlock leftTo = leftRange.getToBlock();
            LongBlock rightFrom = rightRange.getFromBlock();
            LongBlock rightTo = rightRange.getToBlock();
            for (int i = 0; i < leftCount; i++) {
                long aFrom = leftFrom.getLong(leftFirst + i);
                long aTo = leftTo.getLong(leftFirst + i);
                for (int j = 0; j < rightCount; j++) {
                    long bFrom = rightFrom.getLong(rightFirst + j);
                    long bTo = rightTo.getLong(rightFirst + j);
                    if (aFrom >= bFrom && aTo <= bTo) {
                        return true;
                    }
                }
            }
            return false;
        }
        // (date, date_range): point within range
        LongBlock leftLong = (LongBlock) leftBlock;
        LongRangeBlock rightRange = (LongRangeBlock) rightBlock;
        int leftFirst = leftLong.getFirstValueIndex(p);
        int leftCount = leftLong.getValueCount(p);
        int rightFirst = rightRange.getFirstValueIndex(p);
        int rightCount = rightRange.getValueCount(p);
        LongBlock rightFrom = rightRange.getFromBlock();
        LongBlock rightTo = rightRange.getToBlock();
        for (int i = 0; i < leftCount; i++) {
            long point = leftLong.getLong(leftFirst + i);
            for (int j = 0; j < rightCount; j++) {
                long rFrom = rightFrom.getLong(rightFirst + j);
                long rTo = rightTo.getLong(rightFirst + j);
                // Range is [from, to); to is exclusive
                if (point >= rFrom && point < rTo) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + leftEvaluator.baseRamBytesUsed() + rightEvaluator.baseRamBytesUsed();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(leftEvaluator, rightEvaluator);
    }

    @Override
    public String toString() {
        return "RangeWithinEvaluator[left=" + leftEvaluator + ", right=" + rightEvaluator + "]";
    }

    public static class Factory implements ExpressionEvaluator.Factory {
        private final Source source;
        private final DataType leftType;
        private final DataType rightType;
        private final ExpressionEvaluator.Factory left;
        private final ExpressionEvaluator.Factory right;

        public Factory(
            Source source,
            DataType leftType,
            DataType rightType,
            ExpressionEvaluator.Factory left,
            ExpressionEvaluator.Factory right
        ) {
            this.source = source;
            this.leftType = leftType;
            this.rightType = rightType;
            this.left = left;
            this.right = right;
        }

        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new RangeWithinEvaluator(source, leftType, rightType, left.get(context), right.get(context), context);
        }

        @Override
        public String toString() {
            return "RangeWithinEvaluator[left=" + left + ", right=" + right + "]";
        }
    }
}
