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
 * Evaluator for RANGE_WITHIN(left, right) -> boolean.
 * Supports (date_range, date), (date, date_range), (date_range, date_range), (date, date)
 * with Lucene CONTAINS semantics: left contains right (value within range).
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

    private boolean evalPosition(Block leftBlock, Block rightBlock, int p) {
        if (leftType == DataType.DATE_RANGE && rightType == DataType.DATE_RANGE) {
            LongRangeBlock leftRange = (LongRangeBlock) leftBlock;
            LongRangeBlock rightRange = (LongRangeBlock) rightBlock;
            long aFrom = leftRange.getFromBlock().getLong(p);
            long aTo = leftRange.getToBlock().getLong(p);
            long bFrom = rightRange.getFromBlock().getLong(p);
            long bTo = rightRange.getToBlock().getLong(p);
            return bFrom >= aFrom && bTo <= aTo;
        }
        if (leftType == DataType.DATE_RANGE && DataType.isMillisOrNanos(rightType)) {
            LongRangeBlock leftRange = (LongRangeBlock) leftBlock;
            LongBlock rightLong = (LongBlock) rightBlock;
            long from = leftRange.getFromBlock().getLong(p);
            long to = leftRange.getToBlock().getLong(p);
            long point = rightLong.getLong(rightLong.getFirstValueIndex(p));
            return point >= from && point <= to;
        }
        if (DataType.isMillisOrNanos(leftType) && rightType == DataType.DATE_RANGE) {
            LongBlock leftLong = (LongBlock) leftBlock;
            LongRangeBlock rightRange = (LongRangeBlock) rightBlock;
            long point = leftLong.getLong(leftLong.getFirstValueIndex(p));
            long rFrom = rightRange.getFromBlock().getLong(p);
            long rTo = rightRange.getToBlock().getLong(p);
            return point >= rFrom && point <= rTo;
        }
        // (date, date)
        LongBlock leftLong = (LongBlock) leftBlock;
        LongBlock rightLong = (LongBlock) rightBlock;
        long a = leftLong.getLong(leftLong.getFirstValueIndex(p));
        long b = rightLong.getLong(rightLong.getFirstValueIndex(p));
        return a == b;
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
