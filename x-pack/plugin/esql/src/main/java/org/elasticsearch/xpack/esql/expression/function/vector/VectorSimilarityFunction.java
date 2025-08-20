/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.io.IOException;

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
            toEvaluator.apply(left()),
            toEvaluator.apply(right()),
            getSimilarityFunction(),
            getClass().getSimpleName() + "Evaluator"
        );
    }

    /**
     * Returns the similarity function to be used for evaluating the similarity between two vectors.
     */
    protected abstract SimilarityEvaluatorFunction getSimilarityFunction();

    private record SimilarityEvaluatorFactory(
        EvalOperator.ExpressionEvaluator.Factory left,
        EvalOperator.ExpressionEvaluator.Factory right,
        SimilarityEvaluatorFunction similarityFunction,
        String evaluatorName
    ) implements EvalOperator.ExpressionEvaluator.Factory {

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            // TODO check whether to use this custom evaluator or reuse / define an existing one
            return new EvalOperator.ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    try (
                        FloatBlock leftBlock = (FloatBlock) left.get(context).eval(page);
                        FloatBlock rightBlock = (FloatBlock) right.get(context).eval(page)
                    ) {
                        int positionCount = page.getPositionCount();
                        int dimensions = 0;
                        // Get the first non-empty vector to calculate the dimension
                        for (int p = 0; p < positionCount; p++) {
                            if (leftBlock.getValueCount(p) != 0) {
                                dimensions = leftBlock.getValueCount(p);
                                break;
                            }
                        }
                        if (dimensions == 0) {
                            return context.blockFactory().newConstantNullBlock(positionCount);
                        }

                        float[] leftScratch = new float[dimensions];
                        float[] rightScratch = new float[dimensions];
                        try (DoubleBlock.Builder builder = context.blockFactory().newDoubleBlockBuilder(positionCount * dimensions)) {
                            for (int p = 0; p < positionCount; p++) {
                                int dimsLeft = leftBlock.getValueCount(p);
                                int dimsRight = rightBlock.getValueCount(p);

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
                                readFloatArray(leftBlock, leftBlock.getFirstValueIndex(p), dimensions, leftScratch);
                                readFloatArray(rightBlock, rightBlock.getFirstValueIndex(p), dimensions, rightScratch);
                                float result = similarityFunction.calculateSimilarity(leftScratch, rightScratch);
                                builder.appendDouble(result);
                            }
                            return builder.build();
                        }
                    }
                }

                @Override
                public String toString() {
                    return evaluatorName() + "[left=" + left + ", right=" + right + "]";
                }

                @Override
                public void close() {}
            };
        }

        private static void readFloatArray(FloatBlock block, int position, int dimensions, float[] scratch) {
            for (int i = 0; i < dimensions; i++) {
                scratch[i] = block.getFloat(position + i);
            }
        }

        @Override
        public String toString() {
            return evaluatorName() + "[left=" + left + ", right=" + right + "]";
        }
    }
}
