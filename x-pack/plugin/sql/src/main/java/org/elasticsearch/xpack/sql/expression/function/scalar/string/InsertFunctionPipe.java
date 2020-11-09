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

public class InsertFunctionPipe extends Pipe {

    private final Pipe input, start, length, replacement;

    public InsertFunctionPipe(Source source, Expression expression,
            Pipe input, Pipe start,
            Pipe length, Pipe replacement) {
        super(source, expression, Arrays.asList(input, start, length, replacement));
        this.input = input;
        this.start = start;
        this.length = length;
        this.replacement = replacement;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() != 4) {
            throw new IllegalArgumentException("expected [4] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }
    
    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newInput = input.resolveAttributes(resolver);
        Pipe newStart = start.resolveAttributes(resolver);
        Pipe newLength = length.resolveAttributes(resolver);
        Pipe newReplacement = replacement.resolveAttributes(resolver);
        if (newInput == input
                && newStart == start
                && newLength == length
                && newReplacement == replacement) {
            return this;
        }
        return replaceChildren(newInput, newStart, newLength, newReplacement);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return input.supportedByAggsOnlyQuery()
                && start.supportedByAggsOnlyQuery()
                && length.supportedByAggsOnlyQuery()
                && replacement.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return input.resolved() && start.resolved() && length.resolved() && replacement.resolved();
    }
    
    protected Pipe replaceChildren(Pipe newInput,
            Pipe newStart,
            Pipe newLength,
            Pipe newReplacement) {
        return new InsertFunctionPipe(source(), expression(), newInput, newStart, newLength, newReplacement);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
        start.collectFields(sourceBuilder);
        length.collectFields(sourceBuilder);
        replacement.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<InsertFunctionPipe> info() {
        return NodeInfo.create(this, InsertFunctionPipe::new, expression(), input, start, length, replacement);
    }

    @Override
    public InsertFunctionProcessor asProcessor() {
        return new InsertFunctionProcessor(input.asProcessor(), start.asProcessor(), length.asProcessor(), replacement.asProcessor());
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
    
    public Pipe replacement() {
        return replacement;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(input, start, length, replacement);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        InsertFunctionPipe other = (InsertFunctionPipe) obj;
        return Objects.equals(input, other.input)
                && Objects.equals(start, other.start)
                && Objects.equals(length, other.length)
                && Objects.equals(replacement, other.replacement);
    }
}
