/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class LengthFunctionPipe extends Pipe {

    private final Pipe input;

    public LengthFunctionPipe(Source source, Expression expression, Pipe input) {
        super(source, expression, Arrays.asList(input));
        this.input = input;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newInput = input.resolveAttributes(resolver);
        return newInput == input ? this : replaceChildren(newInput);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return input.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return input.resolved();
    }

    protected LengthFunctionPipe replaceChildren(Pipe newInput) {
        return new LengthFunctionPipe(source(), expression(), newInput);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<LengthFunctionPipe> info() {
        return NodeInfo.create(this, LengthFunctionPipe::new, expression(), input);
    }

    @Override
    public LengthFunctionProcessor asProcessor() {
        return new LengthFunctionProcessor(input.asProcessor());
    }

    public Pipe input() {
        return input;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return Objects.equals(input(), ((LengthFunctionPipe) obj).input());
    }
}
