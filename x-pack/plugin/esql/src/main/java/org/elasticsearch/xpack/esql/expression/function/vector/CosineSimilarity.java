/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;

public class CosineSimilarity extends EsqlScalarFunction implements VectorFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "CosineSimilarity",
        CosineSimilarity::new
    );

    private Expression left;
    private Expression right;

    @FunctionInfo(
        returnType = "double",
        preview = true,
        description = "Calculates the cosine similarity between two dense_vectors.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) }
    )
    public CosineSimilarity(Source source, Expression left, Expression right) {
        super(source, List.of(left, right));
        this.left = left;
        this.right = right;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    private CosineSimilarity(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return checkParam(left()).and(checkParam(right()));
    }

    private TypeResolution checkParam(Expression param) {
        return isNotNull(param, sourceText(), FIRST).and(isType(param, dt -> dt == DENSE_VECTOR, sourceText(), FIRST, "dense_vector"));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(left());
        out.writeNamedWriteable(right());
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

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Pow::new, left(), right());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new EvaluatorFactory(toEvaluator.apply(left()), toEvaluator.apply(right()));
    }

    private record EvaluatorFactory(EvalOperator.ExpressionEvaluator.Factory left, EvalOperator.ExpressionEvaluator.Factory right)
        implements
            EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new Evaluator(context, left.get(context), right.get(context));
        }

        @Override
        public String toString() {
            return "CosineSimilarity[left=" + left + ", right=" + right + "]";
        }
    }

    /**
     * Evaluator for {@link CosineSimilarity}. Not generated and doesn’t extend from
     * {@link AbstractMultivalueFunction.AbstractEvaluator} because it’s different from {@link org.elasticsearch.compute.ann.MvEvaluator}
     * or scalar evaluators.
     *
     * We can probably generalize to a common class or use its own annotation / evaluator template
     */
    private static class Evaluator implements EvalOperator.ExpressionEvaluator {
        private final DriverContext context;
        private final EvalOperator.ExpressionEvaluator left;
        private final EvalOperator.ExpressionEvaluator right;

        Evaluator(DriverContext context, EvalOperator.ExpressionEvaluator left, EvalOperator.ExpressionEvaluator right) {
            this.context = context;
            this.left = left;
            this.right = right;
        }

        @Override
        public final Block eval(Page page) {
            try (FloatBlock leftBlock = (FloatBlock) left.eval(page); FloatBlock rightBlock = (FloatBlock) right.eval(page)) {
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
                        float result = VectorSimilarityFunction.COSINE.compare(leftScratch, rightScratch);
                        builder.appendDouble(result);
                    }
                    return builder.build().asBlock();
                }
            }
        }

        private void readFloatArray(FloatBlock block, int position, int dimensions, float[] scratch) {
            for (int i = 0; i < dimensions; i++) {
                scratch[i] = block.getFloat(position + i);
            }
        }

        @Override
        public final String toString() {
            return "CosineSimilarity[left=" + left + ", right=" + right + "]";
        }

        @Override
        public void close() {}
    }

}
