/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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

    private final Pipe pattern, input, start;

    public LocateFunctionPipe(Source source, Expression expression, Pipe pattern, Pipe input, Pipe start) {
        super(source, expression, start == null ? Arrays.asList(pattern, input) : Arrays.asList(pattern, input, start));
        this.pattern = pattern;
        this.input = input;
        this.start = start;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.size() == 2 ? null : newChildren.get(2));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newPattern = pattern.resolveAttributes(resolver);
        Pipe newInput = input.resolveAttributes(resolver);
        Pipe newStart = start == null ? start : start.resolveAttributes(resolver);
        if (newPattern == pattern && newInput == input && newStart == start) {
            return this;
        }
        return replaceChildren(newPattern, newInput, newStart);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return pattern.supportedByAggsOnlyQuery()
            && input.supportedByAggsOnlyQuery()
            && (start == null || start.supportedByAggsOnlyQuery());
    }

    @Override
    public boolean resolved() {
        return pattern.resolved() && input.resolved() && (start == null || start.resolved());
    }

    protected Pipe replaceChildren(Pipe newPattern, Pipe newInput, Pipe newStart) {
        return new LocateFunctionPipe(source(), expression(), newPattern, newInput, newStart);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        pattern.collectFields(sourceBuilder);
        input.collectFields(sourceBuilder);
        if (start != null) {
            start.collectFields(sourceBuilder);
        }
    }

    @Override
    protected NodeInfo<LocateFunctionPipe> info() {
        return NodeInfo.create(this, LocateFunctionPipe::new, expression(), pattern, input, start);
    }

    @Override
    public LocateFunctionProcessor asProcessor() {
        return new LocateFunctionProcessor(pattern.asProcessor(), input.asProcessor(), start == null ? null : start.asProcessor());
    }

    public Pipe input() {
        return input;
    }

    public Pipe start() {
        return start;
    }

    public Pipe pattern() {
        return pattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, input, start);
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
        return Objects.equals(pattern, other.pattern) && Objects.equals(input, other.input) && Objects.equals(start, other.start);
    }
}
