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

public class SubstringFunctionPipe extends Pipe {

    private final Pipe input, start, length;

    public SubstringFunctionPipe(Source source, Expression expression, Pipe input, Pipe start, Pipe length) {
        super(source, expression, Arrays.asList(input, start, length));
        this.input = input;
        this.start = start;
        this.length = length;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newInput = input.resolveAttributes(resolver);
        Pipe newStart = start.resolveAttributes(resolver);
        Pipe newLength = length.resolveAttributes(resolver);
        if (newInput == input && newStart == start && newLength == length) {
            return this;
        }
        return replaceChildren(newInput, newStart, newLength);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return input.supportedByAggsOnlyQuery() && start.supportedByAggsOnlyQuery() && length.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return input.resolved() && start.resolved() && length.resolved();
    }

    protected SubstringFunctionPipe replaceChildren(Pipe newInput, Pipe newStart, Pipe newLength) {
        return new SubstringFunctionPipe(source(), expression(), newInput, newStart, newLength);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
        start.collectFields(sourceBuilder);
        length.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<SubstringFunctionPipe> info() {
        return NodeInfo.create(this, SubstringFunctionPipe::new, expression(), input, start, length);
    }

    @Override
    public SubstringFunctionProcessor asProcessor() {
        return new SubstringFunctionProcessor(input.asProcessor(), start.asProcessor(), length.asProcessor());
    }

    public Pipe input() {
        return input;
    }

    public Pipe start() {
        return start;
    }

    public Pipe length() {
        return length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input, start, length);
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
        return Objects.equals(input, other.input) && Objects.equals(start, other.start) && Objects.equals(length, other.length);
    }
}
