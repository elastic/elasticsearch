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
 * {@link EvalOperator.ExpressionEvaluator} implementation for performing arithmetic operations on lhs=double and rhs=dense_vector arguments.
 *
 */
class DoubleDenseVectorOpEvaluator implements EvalOperator.ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DoubleDenseVectorOpEvaluator.class);
    private static final String ADD_DENSE_VECTOR_EVALUATOR = "AddDoubleAndDenseVectorEvaluator";
    private static final String SUB_DENSE_VECTOR_EVALUATOR = "SubDoubleAndDenseVectorEvaluator";
    private static final String MUL_DENSE_VECTOR_EVALUATOR = "MulDoubleAndDenseVectorEvaluator";
    private static final String DIV_DENSE_VECTOR_EVALUATOR = "DivDoubleAndDenseVectorEvaluator";

    private final BiFunction<Double, Float, Float> op;
    private final String name;
    private final Source source;
    private final double lhs;
    private final EvalOperator.ExpressionEvaluator rhs;
    private final DriverContext driverContext;
    private Warnings warnings;

    DoubleDenseVectorOpEvaluator(
        BiFunction<Double, Float, Float> op,
        String name,
        Source source,
        double lhs,
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
        try (var rhsBlock = (FloatBlock) rhs.eval(page)) {
            int positionCount = page.getPositionCount();
            try (var resultBlock = driverContext.blockFactory().newFloatBlockBuilder(positionCount)) {
                float[] buffer = new float[0];
                for (int p = 0; p < positionCount; p++) {
                    if (rhsBlock.isNull(p)) {
                        resultBlock.appendNull();
                        continue;
                    }

                    int rhsValueCount = rhsBlock.getValueCount(p);
                    if (buffer.length < rhsValueCount) {
                        buffer = new float[rhsValueCount];
                    }
                    int rhsStart = rhsBlock.getFirstValueIndex(p);
                    try {
                        for (int i = 0; i < rhsValueCount; i++) {
                            float rhs = rhsBlock.getFloat(rhsStart + i);
                            buffer[i] = op.apply(lhs, rhs);
                        }
                        resultBlock.beginPositionEntry();
                        for (int i = 0; i < rhsValueCount; i++) {
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
        return BASE_RAM_BYTES_USED + rhs.baseRamBytesUsed();
    }

    @Override
    public String toString() {
        return name + "[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(rhs);
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
        }
        return warnings;
    }

    static final class AddFactory implements Factory {
        private final Source source;
        private final double lhs;
        private final Factory rhs;

        AddFactory(Source source, double lhs, Factory rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DoubleDenseVectorOpEvaluator get(DriverContext context) {
            return new DoubleDenseVectorOpEvaluator(
                DenseVectorsEvaluator::processAdd,
                ADD_DENSE_VECTOR_EVALUATOR,
                source,
                lhs,
                rhs.get(context),
                context
            );
        }

        @Override
        public String toString() {
            return ADD_DENSE_VECTOR_EVALUATOR + "[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
        }
    }

    static class SubFactory implements Factory {
        private final Source source;
        private final double lhs;
        private final Factory rhs;

        SubFactory(Source source, double lhs, Factory rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DoubleDenseVectorOpEvaluator get(DriverContext context) {
            return new DoubleDenseVectorOpEvaluator(
                DenseVectorsEvaluator::processSub,
                SUB_DENSE_VECTOR_EVALUATOR,
                source,
                lhs,
                rhs.get(context),
                context
            );
        }

        @Override
        public String toString() {
            return SUB_DENSE_VECTOR_EVALUATOR + "[lhs=" + lhs + ", rhs=" + rhs + "]";
        }
    }

    static class MulFactory implements Factory {
        private final Source source;
        private final double lhs;
        private final Factory rhs;

        MulFactory(Source source, double lhs, Factory rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DoubleDenseVectorOpEvaluator get(DriverContext context) {
            return new DoubleDenseVectorOpEvaluator(
                DenseVectorsEvaluator::processMul,
                MUL_DENSE_VECTOR_EVALUATOR,
                source,
                lhs,
                rhs.get(context),
                context
            );
        }

        @Override
        public String toString() {
            return MUL_DENSE_VECTOR_EVALUATOR + "[lhs=" + lhs + ", rhs=" + rhs + "]";
        }
    }

    static class DivFactory implements Factory {
        private final Source source;
        private final double lhs;
        private final Factory rhs;

        DivFactory(Source source, double lhs, Factory rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DoubleDenseVectorOpEvaluator get(DriverContext context) {
            return new DoubleDenseVectorOpEvaluator(
                DenseVectorsEvaluator::processDiv,
                DIV_DENSE_VECTOR_EVALUATOR,
                source,
                lhs,
                rhs.get(context),
                context
            );
        }

        @Override
        public String toString() {
            return DIV_DENSE_VECTOR_EVALUATOR + "[lhs=" + lhs + ", rhs=" + rhs + "]";
        }
    }
}
