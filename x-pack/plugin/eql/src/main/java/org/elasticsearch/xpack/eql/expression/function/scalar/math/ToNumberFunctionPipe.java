/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ToNumberFunctionPipe extends Pipe {

    private final Pipe value, base;

    public ToNumberFunctionPipe(Source source, Expression expression, Pipe value, Pipe base) {
        super(source, expression, Arrays.asList(value, base));
        this.value = value;
        this.base = base;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newValue = value.resolveAttributes(resolver);
        Pipe newBase = base.resolveAttributes(resolver);
        if (newValue == value && newBase == base) {
            return this;
        }
        return replaceChildren(newValue, newBase);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return value.supportedByAggsOnlyQuery() && base.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return value.resolved() && base.resolved();
    }

    protected ToNumberFunctionPipe replaceChildren(Pipe newValue, Pipe newBase) {
        return new ToNumberFunctionPipe(source(), expression(), newValue, newBase);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        value.collectFields(sourceBuilder);
        base.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<ToNumberFunctionPipe> info() {
        return NodeInfo.create(this, ToNumberFunctionPipe::new, expression(), value, base);
    }

    @Override
    public ToNumberFunctionProcessor asProcessor() {
        return new ToNumberFunctionProcessor(value.asProcessor(), base.asProcessor());
    }

    public Pipe value() {
        return value;
    }

    public Pipe base() {
        return base;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value(), base());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ToNumberFunctionPipe other = (ToNumberFunctionPipe) obj;
        return Objects.equals(value(), other.value()) && Objects.equals(base(), other.base());
    }
}
