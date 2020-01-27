/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class LocateFunctionPipe extends Pipe {

    private final Pipe pattern, source, start;

    public LocateFunctionPipe(Source source, Expression expression, Pipe pattern,
            Pipe src, Pipe start) {
        super(source, expression, start == null ? Arrays.asList(pattern, src) : Arrays.asList(pattern, src, start));
        this.pattern = pattern;
        this.source = src;
        this.start = start;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        int childrenSize = newChildren.size();
        if (childrenSize > 3 || childrenSize < 2) {
            throw new IllegalArgumentException("expected [2 or 3] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1), childrenSize == 2 ? null : newChildren.get(2));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newPattern = pattern.resolveAttributes(resolver);
        Pipe newSource = source.resolveAttributes(resolver);
        Pipe newStart = start == null ? start : start.resolveAttributes(resolver);
        if (newPattern == pattern && newSource == source && newStart == start) {
            return this;
        }
        return replaceChildren(newPattern, newSource, newStart);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return pattern.supportedByAggsOnlyQuery() && source.supportedByAggsOnlyQuery()
                && (start == null || start.supportedByAggsOnlyQuery());
    }

    @Override
    public boolean resolved() {
        return pattern.resolved() && source.resolved() && (start == null || start.resolved());
    }

    protected Pipe replaceChildren(Pipe newPattern, Pipe newSource,
            Pipe newStart) {
        return new LocateFunctionPipe(source(), expression(), newPattern, newSource, newStart);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        pattern.collectFields(sourceBuilder);
        source.collectFields(sourceBuilder);
        if (start != null) {
            start.collectFields(sourceBuilder);
        }
    }

    @Override
    protected NodeInfo<LocateFunctionPipe> info() {
        return NodeInfo.create(this, LocateFunctionPipe::new, expression(), pattern, source, start);
    }

    @Override
    public LocateFunctionProcessor asProcessor() {
        return new LocateFunctionProcessor(pattern.asProcessor(), source.asProcessor(), start == null ? null : start.asProcessor());
    }
    
    public Pipe src() {
        return source;
    }
    
    public Pipe start() {
        return start;
    }
    
    public Pipe pattern() {
        return pattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, source, start);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LocateFunctionPipe other = (LocateFunctionPipe) obj;
        return Objects.equals(pattern, other.pattern) && Objects.equals(source, other.source) && Objects.equals(start, other.start);
    }
}