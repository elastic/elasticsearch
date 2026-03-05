/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.function.BiFunction;

import static org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for performing arithmetic operations on two dense_vector arguments.
 *
 */
class DenseVectorsEvaluator implements EvalOperator.ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DenseVectorsEvaluator.class);

    private final BiFunction<Float, Float, Float> op;
    private final String name;
    private final Source source;
    private final EvalOperator.ExpressionEvaluator lhs;
    private final EvalOperator.ExpressionEvaluator rhs;
    private final DriverContext driverContext;
    private Warnings warnings;

    DenseVectorsEvaluator(
        BiFunction<Float, Float, Float> op,
        String name,
        Source source,
        EvalOperator.ExpressionEvaluator lhs,
        EvalOperator.ExpressionEvaluator rhs,
        DriverContext driverContext
    ) {
        this.op = op;
        this.name = name;
        this.source = source;
        this.lhs = lhs;
        this.rhs = rhs;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(Page page) {
        try (var lhsBlock = (FloatBlock) lhs.eval(page); var rhsBlock = (FloatBlock) rhs.eval(page)) {
            int positionCount = page.getPositionCount();
            try (var resultBlock = driverContext.blockFactory().newFloatBlockBuilder(positionCount)) {
                float[] buffer = new float[0];
                for (int p = 0; p < positionCount; p++) {
                    if (lhsBlock.isNull(p) || rhsBlock.isNull(p)) {
                        resultBlock.appendNull();
                        continue;
                    }

                    int lhsValueCount = lhsBlock.getValueCount(p);
                    int rhsValueCount = rhsBlock.getValueCount(p);

                    // invalid operation if dimensions do not match
                    if (lhsValueCount != rhsValueCount) {
                        warnings().registerException(new IllegalArgumentException("dense_vector dimensions do not match"));
                        resultBlock.appendNull();
                        continue;
                    }

                    // Perform element-wise operations
                    int lhsStart = lhsBlock.getFirstValueIndex(p);
                    int rhsStart = rhsBlock.getFirstValueIndex(p);
                    if (buffer.length < lhsValueCount) {
                        buffer = new float[lhsValueCount];
                    }
                    try {
                        for (int i = 0; i < lhsValueCount; i++) {
                            float l = lhsBlock.getFloat(lhsStart + i);
                            float r = rhsBlock.getFloat(rhsStart + i);
                            buffer[i] = op.apply(l, r);
                        }
                        resultBlock.beginPositionEntry();
                        for (int i = 0; i < lhsValueCount; i++) {
                            resultBlock.appendFloat(buffer[i]);
                        }
                        resultBlock.endPositionEntry();
                    } catch (ArithmeticException e) {
                        warnings().registerException(e);
                        resultBlock.appendNull();
                    }
                }
                return resultBlock.build();
            }
        }
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + lhs.baseRamBytesUsed() + rhs.baseRamBytesUsed();
    }

    @Override
    public String toString() {
        return "DenseVectorsEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + ", opName=" + name + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(lhs, rhs);
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
        }
        return warnings;
    }

    static final class Factory implements ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory lhs;
        private final EvalOperator.ExpressionEvaluator.Factory rhs;
        private final BiFunction<Float, Float, Float> op;
        private final String opName;

        Factory(
            Source source,
            ExpressionEvaluator.Factory lhs,
            ExpressionEvaluator.Factory rhs,
            BiFunction<Float, Float, Float> op,
            String opName
        ) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
            this.op = op;
            this.opName = opName;
        }

        @Override
        public DenseVectorsEvaluator get(DriverContext context) {
            return new DenseVectorsEvaluator(op, opName, source, lhs.get(context), rhs.get(context), context);
        }

        @Override
        public String toString() {
            return "DenseVectorsEvaluator[" + "lhs=" + lhs + ", rhs=" + rhs + ", opName=" + opName + "]";
        }
    }
}
