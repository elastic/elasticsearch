/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExpressionContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;

public class Magnitude extends UnaryScalarFunction implements EvaluatorMapper, VectorFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Magnitude",
        Magnitude::new
    );
    static final ScalarEvaluatorFunction SCALAR_FUNCTION = Magnitude::calculateScalar;

    @FunctionInfo(
        returnType = "double",
        preview = true,
        description = "Calculates the magnitude of a dense_vector.",
        examples = { @Example(file = "vector-magnitude", tag = "vector-magnitude") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) }
    )
    public Magnitude(
        Source source,
        @Param(name = "input", type = { "dense_vector" }, description = "dense_vector for which to compute the magnitude") Expression input
    ) {
        super(source, input);
    }

    private Magnitude(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected UnaryScalarFunction replaceChild(Expression newChild) {
        return new Magnitude(source(), newChild);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Magnitude::new, field());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public static float calculateScalar(float[] scratch) {
        return (float) Math.sqrt(VectorUtil.dotProduct(scratch, scratch));
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

        return isType(field(), dt -> dt == DENSE_VECTOR, sourceText(), TypeResolutions.ParamOrdinal.FIRST, "dense_vector");
    }

    /**
     * Functional interface for evaluating the scalar value of the underlying float array.
     */
    @FunctionalInterface
    public interface ScalarEvaluatorFunction {
        float calculateScalar(float[] scratch);
    }

    @Override
    public Object fold(ExpressionContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public final EvalOperator.ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        return new ScalarEvaluatorFactory(toEvaluator.apply(field()), SCALAR_FUNCTION, getClass().getSimpleName() + "Evaluator");
    }

    private record ScalarEvaluatorFactory(
        EvalOperator.ExpressionEvaluator.Factory child,
        ScalarEvaluatorFunction scalarFunction,
        String evaluatorName
    ) implements EvalOperator.ExpressionEvaluator.Factory {

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            // TODO check whether to use this custom evaluator or reuse / define an existing one
            return new ScalarEvaluator(child.get(context), scalarFunction, evaluatorName, context.blockFactory());
        }

        @Override
        public String toString() {
            return evaluatorName() + "[child=" + child + "]";
        }
    }

    private record ScalarEvaluator(
        EvalOperator.ExpressionEvaluator child,
        ScalarEvaluatorFunction scalarFunction,
        String evaluatorName,
        BlockFactory blockFactory
    ) implements EvalOperator.ExpressionEvaluator {

        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ScalarEvaluator.class);

        @Override
        public Block eval(Page page) {
            try (FloatBlock block = (FloatBlock) child.eval(page);) {
                int positionCount = page.getPositionCount();
                int dimensions = 0;
                // Get the first non-empty vector to calculate the dimension
                for (int p = 0; p < positionCount; p++) {
                    if (block.getValueCount(p) != 0) {
                        dimensions = block.getValueCount(p);
                        break;
                    }
                }
                if (dimensions == 0) {
                    return blockFactory.newConstantFloatBlockWith(0F, 0);
                }

                float[] scratch = new float[dimensions];
                try (var builder = blockFactory.newDoubleBlockBuilder(positionCount * dimensions)) {
                    for (int p = 0; p < positionCount; p++) {
                        int dims = block.getValueCount(p);
                        if (dims == 0) {
                            // A null value for the vector, by default append null as result.
                            builder.appendNull();
                            continue;
                        }
                        readFloatArray(block, block.getFirstValueIndex(p), dimensions, scratch);
                        float result = scalarFunction.calculateScalar(scratch);
                        builder.appendDouble(result);
                    }
                    return builder.build();
                }
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + child.baseRamBytesUsed();
        }

        @Override
        public String toString() {
            return evaluatorName() + "[child=" + child + "]";
        }

        private static void readFloatArray(FloatBlock block, int position, int dimensions, float[] scratch) {
            for (int i = 0; i < dimensions; i++) {
                scratch[i] = block.getFloat(position + i);
            }
        }

        @Override
        public void close() {
            child.close();
        }
    }
}
