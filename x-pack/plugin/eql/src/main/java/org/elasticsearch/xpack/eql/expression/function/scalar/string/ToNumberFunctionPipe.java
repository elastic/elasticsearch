/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ToNumberFunctionPipe extends Pipe {

    private final Pipe source, base;

    public ToNumberFunctionPipe(Source source, Expression expression, Pipe src, Pipe base) {
        super(source, expression, Arrays.asList(src, base));
        this.source = src;
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
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newSource = source.resolveAttributes(resolver);
        if (newSource == source) {
            return this;
        }
        return replaceChildren(Collections.singletonList(newSource));
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return source.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return source.resolved();
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        source.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<ToNumberFunctionPipe> info() {
        return NodeInfo.create(this, ToNumberFunctionPipe::new, expression(), source, base);
    }

    @Override
    public ToNumberFunctionProcessor asProcessor() {
        return new ToNumberFunctionProcessor(source.asProcessor(), base.asProcessor());
    }

    public Pipe src() {
        return source;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return Objects.equals(source, ((ToNumberFunctionPipe) obj).source);
    }
}
