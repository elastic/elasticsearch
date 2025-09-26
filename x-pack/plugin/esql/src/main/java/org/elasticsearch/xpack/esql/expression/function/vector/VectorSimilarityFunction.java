/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;

/**
 * Base class for vector similarity functions, which compute a similarity score between two dense vectors
 */
public abstract class VectorSimilarityFunction extends BinaryScalarFunction implements EvaluatorMapper, VectorFunction {

    protected VectorSimilarityFunction(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    protected VectorSimilarityFunction(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return checkDenseVectorParam(left(), FIRST).and(checkDenseVectorParam(right(), SECOND));
    }

    private TypeResolution checkDenseVectorParam(Expression param, TypeResolutions.ParamOrdinal paramOrdinal) {
        return isType(param, dt -> dt == DENSE_VECTOR, sourceText(), paramOrdinal, "dense_vector");
    }

    /**
     * Functional interface for evaluating the similarity between two float arrays
     */
    @FunctionalInterface
    public interface SimilarityEvaluatorFunction {
        float calculateSimilarity(float[] leftScratch, float[] rightScratch);
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public final EvalOperator.ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        return new SimilarityEvaluatorFactory(
            left(),
            right(),
            toEvaluator::apply,
            getSimilarityFunction(),
            getClass().getSimpleName() + "Evaluator"
        );
    }

    /**
     * Returns the similarity function to be used for evaluating the similarity between two vectors.
     */
    protected abstract SimilarityEvaluatorFunction getSimilarityFunction();

    @SuppressWarnings("unchecked")
    private record SimilarityEvaluatorFactory(
        Expression leftExpression,
        Expression rightExpression,
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator,
        SimilarityEvaluatorFunction similarityFunction,
        String evaluatorName
    ) implements EvalOperator.ExpressionEvaluator.Factory {

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            VectorValueProvider left;
            VectorValueProvider right;
            if (leftExpression instanceof Literal && leftExpression.dataType() == DENSE_VECTOR) {
                left = new VectorValueProvider((ArrayList<Float>) ((Literal) leftExpression).value(), null);
            } else {
                left = new VectorValueProvider(null, toEvaluator.apply(leftExpression).get(context));
            }
            if (rightExpression instanceof Literal && rightExpression.dataType() == DENSE_VECTOR) {
                right = new VectorValueProvider((ArrayList<Float>) ((Literal) rightExpression).value(), null);
            } else {
                right = new VectorValueProvider(null, toEvaluator.apply(rightExpression).get(context));
            }
            // TODO check whether to use this custom evaluator or reuse / define an existing one
            return new SimilarityEvaluator(left, right, similarityFunction, evaluatorName, context.blockFactory());
        }

        @Override
        public String toString() {
            return evaluatorName() + "[left=" + "left" + ", right=" + "right" + "]";
        }
    }

    private static class VectorValueProvider implements Releasable {

        private final float[] constantVector;
        private final EvalOperator.ExpressionEvaluator expressionEvaluator;
        private FloatBlock block;
        float[] scratch;

        VectorValueProvider(ArrayList<Float> constantVector, EvalOperator.ExpressionEvaluator expressionEvaluator) {
            if (constantVector != null) {
                this.constantVector = new float[constantVector.size()];
                for (int i = 0; i < constantVector.size(); i++) {
                    this.constantVector[i] = constantVector.get(i);
                }
            } else {
                this.constantVector = null;
            }
            this.expressionEvaluator = expressionEvaluator;
        }

        private void eval(Page page) {
            if (expressionEvaluator != null) {
                block = (FloatBlock) expressionEvaluator.eval(page);
            }
        }

        private float[] getVector(int position) {
            if (constantVector != null) {
                return constantVector;
            } else if (block != null) {
                if (block.isNull(position)) {
                    return null;
                }
                if (scratch == null) {
                    int dims = block.getValueCount(position);
                    if (dims > 0) {
                        scratch = new float[dims];
                    }
                }
                if (scratch != null) {
                    readFloatArray(block, block.getFirstValueIndex(position), scratch);
                }
                return scratch;
            } else {
                throw new EsqlClientException("VectorValueProvider not properly initialized. Both [constantVector] and [block] are null.");
            }
        }

        private static void readFloatArray(FloatBlock block, int firstValueIndex, float[] scratch) {
            for (int i = 0; i < scratch.length; i++) {
                scratch[i] = block.getFloat(firstValueIndex + i);
            }
        }

        public int getDimensions() {
            if (constantVector != null) {
                return constantVector.length;
            } else if (block != null) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    int dims = block.getValueCount(p);
                    if (dims > 0) {
                        return dims;
                    }
                }
                return 0;
            } else {
                throw new EsqlClientException("[VectorValueProvider] not initialized");
            }
        }

        public long baseRamBytesUsed() {
            return (constantVector == null ? 0 : RamUsageEstimator.shallowSizeOf(constantVector)) + (expressionEvaluator == null
                ? 0
                : expressionEvaluator.baseRamBytesUsed());
        }

        public void finishBlock() {
            if (block != null) {
                block.close();
                block = null;
                scratch = null;
            }
        }

        public void close() {
            Releasables.close(expressionEvaluator);
        }

        @Override
        public String toString() {
            return "constant_vector=" + Arrays.toString(constantVector) + ", expressionEvaluator=[" + expressionEvaluator + "]";
        }
    }

    private record SimilarityEvaluator(
        VectorValueProvider left,
        VectorValueProvider right,
        SimilarityEvaluatorFunction similarityFunction,
        String evaluatorName,
        BlockFactory blockFactory
    ) implements EvalOperator.ExpressionEvaluator {

        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SimilarityEvaluator.class);

        @Override
        public Block eval(Page page) {
            try {
                left.eval(page);
                right.eval(page);

                int dimensions = left.getDimensions();
                int positionCount = page.getPositionCount();
                if (dimensions == 0) {
                    return blockFactory.newConstantNullBlock(positionCount);
                }

                try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(positionCount)) {
                    for (int p = 0; p < positionCount; p++) {
                        float[] leftVector = left.getVector(p);
                        float[] rightVector = right.getVector(p);

                        int dimsLeft = leftVector == null ? 0 : leftVector.length;
                        int dimsRight = rightVector == null ? 0 : rightVector.length;

                        if (dimsLeft == 0 || dimsRight == 0) {
                            // A null value on the left or right vector. Similarity is null
                            builder.appendNull();
                            continue;
                        } else if (dimsLeft != dimsRight) {
                            throw new EsqlClientException(
                                "Vectors must have the same dimensions; first vector has {}, and second has {}",
                                dimsLeft,
                                dimsRight
                            );
                        }
                        float result = similarityFunction.calculateSimilarity(leftVector, rightVector);
                        builder.appendDouble(result);
                    }
                    return builder.build();
                }
            } finally {
                left.finishBlock();
                right.finishBlock();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + left.baseRamBytesUsed() + right.baseRamBytesUsed();
        }

        @Override
        public String toString() {
            return evaluatorName() + "[left=" + left + ", right=" + right + "]";
        }

        @Override
        public void close() {
            Releasables.close(left, right);
        }
    }
}
