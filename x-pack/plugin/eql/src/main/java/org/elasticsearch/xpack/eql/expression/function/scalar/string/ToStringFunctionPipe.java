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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ToStringFunctionPipe extends Pipe {

    private final Pipe source;

    public ToStringFunctionPipe(Source source, Expression expression, Pipe src) {
        super(source, expression, Collections.singletonList(src));
        this.source = src;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] children but received [" + newChildren.size() + "]");
        }
        return new ToStringFunctionPipe(source(), expression(), newChildren.get(0));
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
    protected NodeInfo<ToStringFunctionPipe> info() {
        return NodeInfo.create(this, ToStringFunctionPipe::new, expression(), source);
    }

    @Override
    public ToStringFunctionProcessor asProcessor() {
        return new ToStringFunctionProcessor(source.asProcessor());
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

        return Objects.equals(source, ((ToStringFunctionPipe) obj).source);
    }
}
