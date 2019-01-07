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

public class InsertFunctionPipe extends Pipe {

    private final Pipe source, start, length, replacement;

    public InsertFunctionPipe(Source source, Expression expression,
            Pipe src, Pipe start,
            Pipe length, Pipe replacement) {
        super(source, expression, Arrays.asList(src, start, length, replacement));
        this.source = src;
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
        Pipe newSource = source.resolveAttributes(resolver);
        Pipe newStart = start.resolveAttributes(resolver);
        Pipe newLength = length.resolveAttributes(resolver);
        Pipe newReplacement = replacement.resolveAttributes(resolver);
        if (newSource == source
                && newStart == start
                && newLength == length
                && newReplacement == replacement) {
            return this;
        }
        return replaceChildren(newSource, newStart, newLength, newReplacement);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return source.supportedByAggsOnlyQuery()
                && start.supportedByAggsOnlyQuery()
                && length.supportedByAggsOnlyQuery()
                && replacement.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return source.resolved() && start.resolved() && length.resolved() && replacement.resolved();
    }
    
    protected Pipe replaceChildren(Pipe newSource,
            Pipe newStart,
            Pipe newLength,
            Pipe newReplacement) {
        return new InsertFunctionPipe(source(), expression(), newSource, newStart, newLength, newReplacement);
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        source.collectFields(sourceBuilder);
        start.collectFields(sourceBuilder);
        length.collectFields(sourceBuilder);
        replacement.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<InsertFunctionPipe> info() {
        return NodeInfo.create(this, InsertFunctionPipe::new, expression(), source, start, length, replacement);
    }

    @Override
    public InsertFunctionProcessor asProcessor() {
        return new InsertFunctionProcessor(source.asProcessor(), start.asProcessor(), length.asProcessor(), replacement.asProcessor());
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
    
    public Pipe replacement() {
        return replacement;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(source, start, length, replacement);
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
        return Objects.equals(source, other.source)
                && Objects.equals(start, other.start)
                && Objects.equals(length, other.length)
                && Objects.equals(replacement, other.replacement);
    }
}
