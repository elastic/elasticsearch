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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ToStringFunctionPipe extends Pipe {

    private final Pipe input;

    public ToStringFunctionPipe(Source source, Expression expression, Pipe input) {
        super(source, expression, Collections.singletonList(input));
        this.input = input;
    }

    @Override
    public final ToStringFunctionPipe replaceChildren(List<Pipe> newChildren) {
        return new ToStringFunctionPipe(source(), expression(), newChildren.get(0));
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

    protected ToStringFunctionPipe replaceChildren(Pipe newInput) {
        return new ToStringFunctionPipe(source(), expression(), newInput);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<ToStringFunctionPipe> info() {
        return NodeInfo.create(this, ToStringFunctionPipe::new, expression(), input);
    }

    @Override
    public ToStringFunctionProcessor asProcessor() {
        return new ToStringFunctionProcessor(input.asProcessor());
    }

    public Pipe input() {
        return input;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return Objects.equals(input(), ((ToStringFunctionPipe) obj).input());
    }
}
