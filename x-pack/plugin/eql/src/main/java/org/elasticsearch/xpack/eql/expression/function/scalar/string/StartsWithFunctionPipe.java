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
import java.util.List;
import java.util.Objects;

public class StartsWithFunctionPipe extends Pipe {

    private final Pipe source;
    private final Pipe pattern;

    public StartsWithFunctionPipe(Source source, Expression expression, Pipe src, Pipe pattern) {
        super(source, expression, Arrays.asList(src, pattern));
        this.source = src;
        this.pattern = pattern;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newSource = source.resolveAttributes(resolver);
        Pipe newPattern = pattern.resolveAttributes(resolver);
        if (newSource == source && newPattern == pattern) {
            return this;
        }
        return replaceChildren(newSource, newPattern);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return source.supportedByAggsOnlyQuery() && pattern.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return source.resolved() && pattern.resolved();
    }

    protected Pipe replaceChildren(Pipe newSource, Pipe newPattern) {
        return new StartsWithFunctionPipe(source(), expression(), newSource, newPattern);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        source.collectFields(sourceBuilder);
        pattern.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<StartsWithFunctionPipe> info() {
        return NodeInfo.create(this, StartsWithFunctionPipe::new, expression(), source, pattern);
    }

    @Override
    public StartsWithFunctionProcessor asProcessor() {
        return new StartsWithFunctionProcessor(source.asProcessor(), pattern.asProcessor());
    }
    
    public Pipe src() {
        return source;
    }

    public Pipe pattern() {
        return pattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StartsWithFunctionPipe other = (StartsWithFunctionPipe) obj;
        return Objects.equals(source, other.source)
                && Objects.equals(pattern, other.pattern);
    }
}