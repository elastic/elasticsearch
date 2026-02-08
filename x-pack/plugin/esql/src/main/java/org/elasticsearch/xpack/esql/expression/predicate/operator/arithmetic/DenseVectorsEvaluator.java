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
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;

import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for performing arithmetic operations on two dense_vector arguments.
 *
 */
class DenseVectorsEvaluator implements EvalOperator.ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DenseVectorsEvaluator.class);
    private static boolean CAST_ALL_TYPES_TO_DOUBLE = true;
    private static final String ADD_DENSE_VECTOR_EVALUATOR = "AddDenseVectorsEvaluator";
    private static final String SUB_DENSE_VECTOR_EVALUATOR = "SubDenseVectorsEvaluator";
    private static final String MUL_DENSE_VECTOR_EVALUATOR = "MulDenseVectorsEvaluator";
    private static final String DIV_DENSE_VECTOR_EVALUATOR = "DivDenseVectorsEvaluator";

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
                    boolean success = true;
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
                        success = false;
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
        return name + "[" + "lhs=" + lhs + ", rhs=" + rhs + "]";
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

    private static float processAdd(float lhs, float rhs) {
        return NumericUtils.asFiniteNumber(lhs + rhs);
    }

    private static float processSub(float lhs, float rhs) {
        return NumericUtils.asFiniteNumber(lhs - rhs);
    }

    private static float processMul(float lhs, float rhs) {
        return NumericUtils.asFiniteNumber(lhs * rhs);
    }

    private static float processDiv(float lhs, float rhs) {
        float result = lhs / rhs;
        if (Double.isNaN(result) || Double.isInfinite(result)) {
            throw new ArithmeticException("/ by zero");
        }
        return result;
    }

    static final class AddFactory implements Factory {
        private final Source source;
        private final Factory lhs;
        private final Factory rhs;

        AddFactory(Source source, Factory lhs, Factory rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DenseVectorsEvaluator get(DriverContext context) {
            return new DenseVectorsEvaluator(
                DenseVectorsEvaluator::processAdd,
                ADD_DENSE_VECTOR_EVALUATOR,
                source,
                lhs.get(context),
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
        private final Factory lhs;
        private final Factory rhs;

        SubFactory(Source source, Factory lhs, Factory rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DenseVectorsEvaluator get(DriverContext context) {
            return new DenseVectorsEvaluator(
                DenseVectorsEvaluator::processSub,
                SUB_DENSE_VECTOR_EVALUATOR,
                source,
                lhs.get(context),
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
        private final Factory lhs;
        private final Factory rhs;

        MulFactory(Source source, Factory lhs, Factory rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DenseVectorsEvaluator get(DriverContext context) {
            return new DenseVectorsEvaluator(
                DenseVectorsEvaluator::processMul,
                MUL_DENSE_VECTOR_EVALUATOR,
                source,
                lhs.get(context),
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
        private final Factory lhs;
        private final Factory rhs;

        DivFactory(Source source, Factory lhs, Factory rhs) {
            this.source = source;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public DenseVectorsEvaluator get(DriverContext context) {
            return new DenseVectorsEvaluator(
                DenseVectorsEvaluator::processDiv,
                DIV_DENSE_VECTOR_EVALUATOR,
                source,
                lhs.get(context),
                rhs.get(context),
                context
            );
        }

        @Override
        public String toString() {
            return DIV_DENSE_VECTOR_EVALUATOR + "[lhs=" + lhs + ", rhs=" + rhs + "]";
        }
    }

    public static ExpressionEvaluator.Factory getAddFactory(
        Source source,
        DataType lhsType,
        DataType rhsType,
        ExpressionEvaluator.Factory lhsfactory,
        ExpressionEvaluator.Factory rhsFactory
    ) {
        if (lhsType == DENSE_VECTOR && rhsType == DENSE_VECTOR) {
            return new DenseVectorsEvaluator.AddFactory(source, lhsfactory, rhsFactory);
        } else {
            if (lhsType != DENSE_VECTOR) {
                // lhs is a scalar type
                if (CAST_ALL_TYPES_TO_DOUBLE) {
                    // here we cast all types to Double
                    var lhs = Cast.cast(source, lhsType, DOUBLE, lhsfactory);
                    return new DoubleDenseVectorOpEvaluator.AddFactory(source, lhs, rhsFactory);
                }
                return switch (lhsType) {
                    case DOUBLE -> new AddDoubleDenseVectorEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case INTEGER -> new AddIntDenseVectorEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case LONG -> new AddLongDenseVectorEvaluator.Factory(source, lhsfactory, lhsfactory);
                    default -> throw new IllegalArgumentException("");
                };
            } else {
                // rhs is a scalar type
                if (CAST_ALL_TYPES_TO_DOUBLE) {
                    // here we cast all types to Double
                    var rhs = Cast.cast(source, rhsType, DOUBLE, rhsFactory);
                    return new DenseVectorDoubleOpEvaluator.AddFactory(source, lhsfactory, rhs);
                }
                return switch (rhsType) {
                    case DOUBLE -> new AddDenseVectorDoubleEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case INTEGER -> new AddDenseVectorIntEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case LONG -> new AddDenseVectorLongEvaluator.Factory(source, lhsfactory, rhsFactory);
                    default -> throw new IllegalArgumentException("");
                };
            }
        }
    }

    public static ExpressionEvaluator.Factory getSubFactory(
        Source source,
        DataType lhsType,
        DataType rhsType,
        ExpressionEvaluator.Factory lhsfactory,
        ExpressionEvaluator.Factory rhsFactory
    ) {
        if (lhsType == DENSE_VECTOR && rhsType == DENSE_VECTOR) {
            return new DenseVectorsEvaluator.SubFactory(source, lhsfactory, rhsFactory);
        } else {
            if (lhsType != DENSE_VECTOR) {
                // lhs is a scalar type
                if (CAST_ALL_TYPES_TO_DOUBLE) {
                    // here we cast all types to Double
                    var lhs = Cast.cast(source, lhsType, DOUBLE, lhsfactory);
                    return new DoubleDenseVectorOpEvaluator.SubFactory(source, lhs, rhsFactory);
                }
                return switch (lhsType) {
                    case DOUBLE -> new SubDoubleDenseVectorEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case INTEGER -> new SubIntDenseVectorEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case LONG -> new SubLongDenseVectorEvaluator.Factory(source, lhsfactory, lhsfactory);
                    default -> throw new IllegalArgumentException("");
                };
            } else {
                // rhs is a scalar type
                if (CAST_ALL_TYPES_TO_DOUBLE) {
                    // here we cast all types to Double
                    var rhs = Cast.cast(source, rhsType, DOUBLE, rhsFactory);
                    return new DenseVectorDoubleOpEvaluator.SubFactory(source, lhsfactory, rhs);
                }
                return switch (rhsType) {
                    case DOUBLE -> new SubDenseVectorDoubleEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case INTEGER -> new SubDenseVectorIntEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case LONG -> new SubDenseVectorLongEvaluator.Factory(source, lhsfactory, rhsFactory);
                    default -> throw new IllegalArgumentException("");
                };
            }
        }
    }

    public static ExpressionEvaluator.Factory getMulFactory(
        Source source,
        DataType lhsType,
        DataType rhsType,
        ExpressionEvaluator.Factory lhsfactory,
        ExpressionEvaluator.Factory rhsFactory
    ) {
        if (lhsType == DENSE_VECTOR && rhsType == DENSE_VECTOR) {
            return new DenseVectorsEvaluator.MulFactory(source, lhsfactory, rhsFactory);
        } else {
            if (lhsType != DENSE_VECTOR) {
                // lhs is a scalar type
                if (CAST_ALL_TYPES_TO_DOUBLE) {
                    // here we cast all types to Double
                    var lhs = Cast.cast(source, lhsType, DOUBLE, lhsfactory);
                    return new DoubleDenseVectorOpEvaluator.MulFactory(source, lhs, rhsFactory);
                }
                return switch (lhsType) {
                    case DOUBLE -> new MulDoubleDenseVectorEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case INTEGER -> new MulIntDenseVectorEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case LONG -> new MulLongDenseVectorEvaluator.Factory(source, lhsfactory, lhsfactory);
                    default -> throw new IllegalArgumentException("");
                };
            } else {
                // rhs is a scalar type
                if (CAST_ALL_TYPES_TO_DOUBLE) {
                    // here we cast all types to Double
                    var rhs = Cast.cast(source, rhsType, DOUBLE, rhsFactory);
                    return new DenseVectorDoubleOpEvaluator.MulFactory(source, lhsfactory, rhs);
                }
                return switch (rhsType) {
                    case DOUBLE -> new MulDenseVectorDoubleEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case INTEGER -> new MulDenseVectorIntEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case LONG -> new MulDenseVectorLongEvaluator.Factory(source, lhsfactory, rhsFactory);
                    default -> throw new IllegalArgumentException("");
                };
            }
        }
    }

    public static ExpressionEvaluator.Factory getDivFactory(
        Source source,
        DataType lhsType,
        DataType rhsType,
        ExpressionEvaluator.Factory lhsfactory,
        ExpressionEvaluator.Factory rhsFactory
    ) {
        if (lhsType == DENSE_VECTOR && rhsType == DENSE_VECTOR) {
            return new DenseVectorsEvaluator.DivFactory(source, lhsfactory, rhsFactory);
        } else {
            if (lhsType != DENSE_VECTOR) {
                // lhs is a scalar type
                if (CAST_ALL_TYPES_TO_DOUBLE) {
                    // here we cast all types to Double
                    var lhs = Cast.cast(source, lhsType, DOUBLE, lhsfactory);
                    return new DoubleDenseVectorOpEvaluator.DivFactory(source, lhs, rhsFactory);
                }
                return switch (lhsType) {
                    case DOUBLE -> new DivDoubleDenseVectorEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case INTEGER -> new DivIntDenseVectorEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case LONG -> new DivLongDenseVectorEvaluator.Factory(source, lhsfactory, lhsfactory);
                    default -> throw new IllegalArgumentException("");
                };
            } else {
                // rhs is a scalar type
                if (CAST_ALL_TYPES_TO_DOUBLE) {
                    // here we cast all types to Double
                    var rhs = Cast.cast(source, rhsType, DOUBLE, rhsFactory);
                    return new DenseVectorDoubleOpEvaluator.DivFactory(source, lhsfactory, rhs);
                }
                return switch (rhsType) {
                    case DOUBLE -> new DivDenseVectorDoubleEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case INTEGER -> new DivDenseVectorIntEvaluator.Factory(source, lhsfactory, rhsFactory);
                    case LONG -> new DivDenseVectorLongEvaluator.Factory(source, lhsfactory, rhsFactory);
                    default -> throw new IllegalArgumentException("");
                };
            }
        }
    }
}
