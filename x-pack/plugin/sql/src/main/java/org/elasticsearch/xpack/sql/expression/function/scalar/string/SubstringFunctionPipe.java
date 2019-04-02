/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SubstringFunctionPipe extends Pipe {

    private final Pipe source, start, length;

    public SubstringFunctionPipe(Source source, Expression expression, Pipe src,
            Pipe start, Pipe length) {
        super(source, expression, Arrays.asList(src, start, length));
        this.source = src;
        this.start = start;
        this.length = length;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() != 3) {
            throw new IllegalArgumentException("expected [3] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newSource = source.resolveAttributes(resolver);
        Pipe newStart = start.resolveAttributes(resolver);
        Pipe newLength = length.resolveAttributes(resolver);
        if (newSource == source && newStart == start && newLength == length) {
            return this;
        }
        return replaceChildren(newSource, newStart, newLength);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return source.supportedByAggsOnlyQuery() && start.supportedByAggsOnlyQuery() && length.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return source.resolved() && start.resolved() && length.resolved();
    }

    protected Pipe replaceChildren(Pipe newSource, Pipe newStart,
            Pipe newLength) {
        return new SubstringFunctionPipe(source(), expression(), newSource, newStart, newLength);
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        source.collectFields(sourceBuilder);
        start.collectFields(sourceBuilder);
        length.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<SubstringFunctionPipe> info() {
        return NodeInfo.create(this, SubstringFunctionPipe::new, expression(), source, start, length);
    }

    @Override
    public SubstringFunctionProcessor asProcessor() {
        return new SubstringFunctionProcessor(source.asProcessor(), start.asProcessor(), length.asProcessor());
    }
    
    public Pipe src() {
        return source;
    }
    
    public Pipe start() {
        return start;
    }
    
    public Pipe length() {
        return length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, start, length);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SubstringFunctionPipe other = (SubstringFunctionPipe) obj;
        return Objects.equals(source, other.source) && Objects.equals(start, other.start) && Objects.equals(length, other.length);
    }
}
