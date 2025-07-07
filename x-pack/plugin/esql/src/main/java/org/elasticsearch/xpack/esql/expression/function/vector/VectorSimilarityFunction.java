/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;

/**
 * Base class for vector similarity functions, which compute a similarity score between two dense vectors
 */
public abstract class VectorSimilarityFunction extends EsqlScalarFunction implements VectorFunction {

    private final Expression left;
    private final Expression right;

    protected VectorSimilarityFunction(Source source, Expression left, Expression right) {
        super(source, List.of(left, right));
        this.left = left;
        this.right = right;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(left());
        out.writeNamedWriteable(right());
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
        return isNotNull(param, sourceText(), paramOrdinal).and(
            isType(param, dt -> dt == DENSE_VECTOR, sourceText(), paramOrdinal, "dense_vector")
        );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new CosineSimilarity(source(), newChildren.get(0), newChildren.get(1));
    }

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    /**
     * Functional interface for evaluating the similarity between two float arrays
     */
    @FunctionalInterface
    public interface SimilarityEvaluatorFunction {
        float calculateSimilarity(float[] leftScratch, float[] rightScratch);
    }

    @Override
    public final EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new SimilarityEvaluatorFactory(toEvaluator.apply(left()), toEvaluator.apply(right()), getSimilarityFunction());
    }

    /**
     * Returns the similarity function to be used for evaluating the similarity between two vectors.
     */
    protected abstract SimilarityEvaluatorFunction getSimilarityFunction();

    private record SimilarityEvaluatorFactory(
        EvalOperator.ExpressionEvaluator.Factory left,
        EvalOperator.ExpressionEvaluator.Factory right,
        SimilarityEvaluatorFunction similarityFunction
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
                        if (positionCount == 0) {
                            return context.blockFactory().newConstantFloatBlockWith(0F, 0);
                        }

                        int dimensions = leftBlock.getValueCount(0);
                        int dimsRight = rightBlock.getValueCount(0);
                        assert dimensions == dimsRight
                            : "Left and right vector must have the same value count, but got left: " + dimensions + ", right: " + dimsRight;
                        float[] leftScratch = new float[dimensions];
                        float[] rightScratch = new float[dimensions];
                        try (DoubleVector.Builder builder = context.blockFactory().newDoubleVectorBuilder(positionCount * dimensions)) {
                            for (int p = 0; p < positionCount; p++) {
                                assert leftBlock.getValueCount(p) == dimensions
                                    : "Left vector must have the same value count for all positions, but got left: "
                                        + leftBlock.getValueCount(p)
                                        + ", expected: "
                                        + dimensions;
                                assert rightBlock.getValueCount(p) == dimensions
                                    : "Left vector must have the same value count for all positions, but got left: "
                                        + rightBlock.getValueCount(p)
                                        + ", expected: "
                                        + dimensions;

                                readFloatArray(leftBlock, leftBlock.getFirstValueIndex(p), dimensions, leftScratch);
                                readFloatArray(rightBlock, rightBlock.getFirstValueIndex(p), dimensions, rightScratch);
                                float result = similarityFunction.calculateSimilarity(leftScratch, rightScratch);
                                builder.appendDouble(result);
                            }
                            return builder.build().asBlock();
                        }
                    }
                }

                @Override
                public void close() {}

                @Override
                public String toString() {
                    return "ExpressionEvaluator[left=" + left + ", right=" + right + "]";
                }
            };
        }

        private static void readFloatArray(FloatBlock block, int position, int dimensions, float[] scratch) {
            for (int i = 0; i < dimensions; i++) {
                scratch[i] = block.getFloat(position + i);
            }
        }
    }
}
