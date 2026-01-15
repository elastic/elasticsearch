/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for comparing dense_vector equality.
 * Two dense vectors are considered equal if they have the same dimensions and all elements are equal.
 */
public final class EqualsDenseVectorEvaluator implements EvalOperator.ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(EqualsDenseVectorEvaluator.class);

    private final Source source;
    private final EvalOperator.ExpressionEvaluator lhs;
    private final EvalOperator.ExpressionEvaluator rhs;
    private final DriverContext driverContext;

    public EqualsDenseVectorEvaluator(
        Source source,
        EvalOperator.ExpressionEvaluator lhs,
        EvalOperator.ExpressionEvaluator rhs,
        DriverContext driverContext
    ) {
        this.source = source;
        this.lhs = lhs;
        this.rhs = rhs;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(Page page) {
        try (FloatBlock lhsBlock = (FloatBlock) lhs.eval(page); FloatBlock rhsBlock = (FloatBlock) rhs.eval(page)) {
            int positionCount = page.getPositionCount();
            try (BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    int lhsValueCount = lhsBlock.getValueCount(p);
                    int rhsValueCount = rhsBlock.getValueCount(p);

                    // If either is null, result is null
                    if (lhsBlock.isNull(p) || rhsBlock.isNull(p)) {
                        result.appendNull();
                        continue;
                    }

                    // If dimensions differ, vectors are not equal
                    if (lhsValueCount != rhsValueCount) {
                        result.appendBoolean(false);
                        continue;
                    }

                    // Compare all elements
                    boolean equal = true;
                    int lhsStart = lhsBlock.getFirstValueIndex(p);
                    int rhsStart = rhsBlock.getFirstValueIndex(p);
                    for (int i = 0; i < lhsValueCount; i++) {
                        if (lhsBlock.getFloat(lhsStart + i) != rhsBlock.getFloat(rhsStart + i)) {
                            equal = false;
                            break;
                        }
                    }
                    result.appendBoolean(equal);
                }
                return result.build();
            }
        }
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + lhs.baseRamBytesUsed() + rhs.baseRamBytesUsed();
    }

    @Override
    public String toString() {
        return "EqualsDenseVectorEvaluator[lhs=" + lhs + ", rhs=" + rhs + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(lhs, rhs);
    }

    public static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory lhs;
        private final EvalOperator.ExpressionEvaluator.Factory rhs;

        public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory lhs, EvalOperator.ExpressionEvaluator.Factory rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public EqualsDenseVectorEvaluator get(DriverContext context) {
            return new EqualsDenseVectorEvaluator(source, lhs.get(context), rhs.get(context), context);
        }

        @Override
        public String toString() {
            return "EqualsDenseVectorEvaluator[lhs=" + lhs + ", rhs=" + rhs + "]";
        }
    }
}
