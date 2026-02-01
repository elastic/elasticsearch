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

import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DenseVectorsEvaluator.ADD_DENSE_VECTOR_EVALUATOR;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DenseVectorsEvaluator.DIV_DENSE_VECTOR_EVALUATOR;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DenseVectorsEvaluator.MUL_DENSE_VECTOR_EVALUATOR;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DenseVectorsEvaluator.SUB_DENSE_VECTOR_EVALUATOR;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for performing arithmetic operations on lhs=dense_vector and rhs=double argument.
 *
 */
class DenseVectorDoubleOpEvaluator implements EvalOperator.ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DenseVectorDoubleOpEvaluator.class);

    private final BiFunction<Float, Double, Float> op;
    private final String name;
    private final Source source;
    private final EvalOperator.ExpressionEvaluator lhs;
    private final double rhs;
    private final DriverContext driverContext;
    private Warnings warnings;

    DenseVectorDoubleOpEvaluator(
        BiFunction<Float, Double, Float> op,
        String name,
        Source source,
        EvalOperator.ExpressionEvaluator lhs,
        double rhs,
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
                            float lhs = lhsBlock.getFloat(lhsStart + i);
                            buffer[i] = op.apply(lhs, rhs);
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
        return BASE_RAM_BYTES_USED + lhs.baseRamBytesUsed();
    }

    @Override
    public String toString() {
        return name + "[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
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

    static final class AddFactory implements Factory {
        private final Source source;
        private final Factory lhs;
        private final double rhs;

        AddFactory(Source source, Factory lhs, double rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DenseVectorDoubleOpEvaluator get(DriverContext context) {
            return new DenseVectorDoubleOpEvaluator(
                DenseVectorsEvaluator::processAdd,
                ADD_DENSE_VECTOR_EVALUATOR,
                source,
                lhs.get(context),
                rhs,
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
        private final Factory lhs;
        private final double rhs;

        SubFactory(Source source, Factory lhs, double rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DenseVectorDoubleOpEvaluator get(DriverContext context) {
            return new DenseVectorDoubleOpEvaluator(
                DenseVectorsEvaluator::processSub,
                SUB_DENSE_VECTOR_EVALUATOR,
                source,
                lhs.get(context),
                rhs,
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
        private final Factory lhs;
        private final double rhs;

        MulFactory(Source source, Factory lhs, double rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DenseVectorDoubleOpEvaluator get(DriverContext context) {
            return new DenseVectorDoubleOpEvaluator(
                DenseVectorsEvaluator::processMul,
                MUL_DENSE_VECTOR_EVALUATOR,
                source,
                lhs.get(context),
                rhs,
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
        private final Factory lhs;
        private final double rhs;

        DivFactory(Source source, Factory lhs, double rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DenseVectorDoubleOpEvaluator get(DriverContext context) {
            return new DenseVectorDoubleOpEvaluator(
                DenseVectorsEvaluator::processDiv,
                DIV_DENSE_VECTOR_EVALUATOR,
                source,
                lhs.get(context),
                rhs,
                context
            );
        }

        @Override
        public String toString() {
            return DIV_DENSE_VECTOR_EVALUATOR + "[lhs=" + lhs + ", rhs=" + rhs + "]";
        }
    }
}
