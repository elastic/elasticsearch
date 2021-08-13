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

public class SubstringFunctionPipe extends Pipe {

    private final Pipe input, start, end;

    public SubstringFunctionPipe(Source source, Expression expression, Pipe input, Pipe start, Pipe end) {
        super(source, expression, Arrays.asList(input, start, end));
        this.input = input;
        this.start = start;
        this.end = end;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newInput = input.resolveAttributes(resolver);
        Pipe newStart = start.resolveAttributes(resolver);
        Pipe newEnd = end.resolveAttributes(resolver);
        if (newInput == input && newStart == start && newEnd == end) {
            return this;
        }
        return replaceChildren(newInput, newStart, newEnd);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return input.supportedByAggsOnlyQuery() && start.supportedByAggsOnlyQuery() && end.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return input.resolved() && start.resolved() && end.resolved();
    }

    protected SubstringFunctionPipe replaceChildren(Pipe newInput, Pipe newStart, Pipe newEnd) {
        return new SubstringFunctionPipe(source(), expression(), newInput, newStart, newEnd);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
        start.collectFields(sourceBuilder);
        end.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<SubstringFunctionPipe> info() {
        return NodeInfo.create(this, SubstringFunctionPipe::new, expression(), input, start, end);
    }

    @Override
    public SubstringFunctionProcessor asProcessor() {
        return new SubstringFunctionProcessor(input.asProcessor(), start.asProcessor(), end.asProcessor());
    }

    public Pipe input() {
        return input;
    }

    public Pipe start() {
        return start;
    }

    public Pipe end() {
        return end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input(), start(), end());
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
        return Objects.equals(input(), other.input())
                && Objects.equals(start(), other.start())
                && Objects.equals(end(), other.end());
    }
}
