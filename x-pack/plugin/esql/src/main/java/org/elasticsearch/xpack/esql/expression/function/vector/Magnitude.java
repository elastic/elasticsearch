/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;

public class Magnitude extends VectorScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Hamming", Magnitude::new);
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
    protected ScalarEvaluatorFunction getScalarFunction() {
        return SCALAR_FUNCTION;
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
}
