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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;

public abstract class VectorSimilarityFunction extends EsqlScalarFunction implements VectorFunction {
    protected Expression left;
    protected Expression right;

    public VectorSimilarityFunction(
        Source source,
        List<Expression> fields,
        @Param(name = "left", type = { "dense_vector" }, description = "first dense_vector to calculate cosine similarity") Expression left,
        @Param(
            name = "right",
            type = { "dense_vector" },
            description = "second dense_vector to calculate cosine similarity"
        ) Expression right
    ) {
        super(source, fields);
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

        return checkDenseVectorParam(left()).and(checkDenseVectorParam(right()));
    }

    private TypeResolution checkDenseVectorParam(Expression param) {
        return isNotNull(param, sourceText(), FIRST).and(isType(param, dt -> dt == DENSE_VECTOR, sourceText(), FIRST, "dense_vector"));
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

    @FunctionalInterface
    public interface SimilarityEvaluatorFunction {
        float calculateSimilarity(float[] leftScratch, float[] rightScratch);
    }

    protected class SimilarityEvaluatorFactory implements EvalOperator.ExpressionEvaluator.Factory {

        private final EvalOperator.ExpressionEvaluator.Factory left;
        private final EvalOperator.ExpressionEvaluator.Factory right;
        private final SimilarityEvaluatorFunction similarityFunction;

        SimilarityEvaluatorFactory(
            EvalOperator.ExpressionEvaluator.Factory left,
            EvalOperator.ExpressionEvaluator.Factory right,
            SimilarityEvaluatorFunction similarityFunction
        ) {
            this.left = left;
            this.right = right;
            this.similarityFunction = similarityFunction;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
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
