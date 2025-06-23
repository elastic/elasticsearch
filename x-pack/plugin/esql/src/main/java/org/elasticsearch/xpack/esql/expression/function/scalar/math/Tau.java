/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

/**
 * Function that emits tau, also known as 2 * pi.
 */
public class Tau extends DoubleConstantFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Tau", Tau::new);

    public static final double TAU = Math.PI * 2;

    @FunctionInfo(
        returnType = "double",
        description = "Returns the [ratio](https://tauday.com/tau-manifesto) of a circle’s circumference to its radius.",
        examples = @Example(file = "math", tag = "tau")
    )
    public Tau(Source source) {
        super(source);
    }

    private Tau(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Object fold(FoldContext ctx) {
        return TAU;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Tau(source());
    }
}
