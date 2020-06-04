/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.List;

public class ToNumberFunctionPipe extends Pipe {

    private final Pipe value, base;

    public ToNumberFunctionPipe(Source source, Expression expression, Pipe value, Pipe base) {
        super(source, expression, Arrays.asList(value, base));
        this.value = value;
        this.base = base;

    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
        return new ToNumberFunctionPipe(source(), expression(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<ToNumberFunctionPipe> info() {
        return NodeInfo.create(this, ToNumberFunctionPipe::new, expression(), value, base);
    }

    @Override
    public ToNumberFunctionProcessor asProcessor() {
        return new ToNumberFunctionProcessor(value.asProcessor(), base.asProcessor());
    }
}
