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

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for performing arithmetic operations when
 * lhs is a dense_vector and rhs a scalar or vice versa.
 *
 */
class DenseVectorScalarEvaluator implements EvalOperator.ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DenseVectorScalarEvaluator.class);

    private final BiFunction<Float, Float, Float> op;
    private final String name;
    private final Source source;
    private final EvalOperator.ExpressionEvaluator lhs;
    private final Float rhs;
    private final DriverContext driverContext;
    private Warnings warnings;

    DenseVectorScalarEvaluator(
        BiFunction<Float, Float, Float> op,
        String name,
        Source source,
        EvalOperator.ExpressionEvaluator lhs,
        Float rhs,
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
        assert rhs != null : "Operand for dense vector arithmetic operation cannot be null";
        try (var lhsBlock = (FloatBlock) lhs.eval(page)) {
            int positionCount = page.getPositionCount();
            try (var resultBlock = driverContext.blockFactory().newFloatBlockBuilder(positionCount)) {
                float[] buffer = new float[0];
                for (int p = 0; p < positionCount; p++) {
                    if (lhsBlock.isNull(p)) {
                        resultBlock.appendNull();
                        continue;
                    }

                    int lhsValueCount = lhsBlock.getValueCount(p);
                    if (buffer.length < lhsValueCount) {
                        buffer = new float[lhsValueCount];
                    }
                    int lhsStart = lhsBlock.getFirstValueIndex(p);
                    try {
                        for (int i = 0; i < lhsValueCount; i++) {
                            float l = lhsBlock.getFloat(lhsStart + i);
                            // Always assume the scalar operand is the rhs in the processing.
                            // We need to flip the order of arguments for non-commutative operations in the Factory when the scalar is
                            // the lhs, to ensure the correct order of arguments is applied here.
                            buffer[i] = op.apply(l, rhs);
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
        return BASE_RAM_BYTES_USED + lhs.baseRamBytesUsed() + RamUsageEstimator.shallowSizeOfInstance(Float.class);
    }

    @Override
    public String toString() {
        return "DenseVectorScalarEvaluator[" + "lhs=" + lhs + ", rhs=scalar_constant" + ", opName=" + name + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(lhs);
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
        }
        return warnings;
    }

    static final class Factory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory vector;
        private final Float scalar;
        private final BiFunction<Float, Float, Float> op;
        private final String opName;

        // Factory when lhs is a dense_vector and rhs a scalar
        Factory(Source source, EvalOperator.ExpressionEvaluator.Factory lhs, Float rhs, BiFunction<Float, Float, Float> op, String opName) {
            this.source = source;
            this.vector = lhs;
            this.scalar = rhs;
            this.op = op;
            this.opName = opName;
        }

        // Factory when lhs is a scalar and rhs a dense_vector.
        Factory(Source source, Float lhs, EvalOperator.ExpressionEvaluator.Factory rhs, BiFunction<Float, Float, Float> op, String opName) {
            this.source = source;
            this.scalar = lhs;
            this.vector = rhs;
            // flip the order of arguments for scalar-vector operations, as we assume the scalar is always the rhs in the processing
            this.op = (a, b) -> op.apply(b, a);
            this.opName = opName;
        }

        @Override
        public DenseVectorScalarEvaluator get(DriverContext context) {
            return new DenseVectorScalarEvaluator(op, opName, source, vector.get(context), scalar, context);
        }

        @Override
        public String toString() {
            return "DenseVectorScalarEvaluator[" + "lhs=" + vector + ", rhs=scalar_constant, opName=" + opName + "]";
        }
    }

}
