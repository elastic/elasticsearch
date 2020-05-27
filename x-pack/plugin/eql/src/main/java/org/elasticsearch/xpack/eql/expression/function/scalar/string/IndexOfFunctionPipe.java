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

public class IndexOfFunctionPipe extends Pipe {

    private final Pipe source, substring, start;

    public IndexOfFunctionPipe(Source source, Expression expression, Pipe src, Pipe substring, Pipe start) {
        super(source, expression, Arrays.asList(src, substring, start));
        this.source = src;
        this.substring = substring;
        this.start = start;
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
        Pipe newSubstring = substring.resolveAttributes(resolver);
        Pipe newStart = start.resolveAttributes(resolver);
        if (newSource == source && newSubstring == substring && newStart == start) {
            return this;
        }
        return replaceChildren(newSource, newSubstring, newStart);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return source.supportedByAggsOnlyQuery() && substring.supportedByAggsOnlyQuery() && start.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return source.resolved() && substring.resolved() && start.resolved();
    }

    protected Pipe replaceChildren(Pipe newSource, Pipe newSubstring, Pipe newStart) {
        return new IndexOfFunctionPipe(source(), expression(), newSource, newSubstring, newStart);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        source.collectFields(sourceBuilder);
        substring.collectFields(sourceBuilder);
        start.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<IndexOfFunctionPipe> info() {
        return NodeInfo.create(this, IndexOfFunctionPipe::new, expression(), source, substring, start);
    }

    @Override
    public IndexOfFunctionProcessor asProcessor() {
        return new IndexOfFunctionProcessor(source.asProcessor(), substring.asProcessor(), start.asProcessor());
    }
    
    public Pipe src() {
        return source;
    }

    public Pipe substring() {
        return substring;
    }

    public Pipe start() {
        return start;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, substring, start);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        IndexOfFunctionPipe other = (IndexOfFunctionPipe) obj;
        return Objects.equals(source, other.source)
                && Objects.equals(substring, other.substring)
                && Objects.equals(start, other.start);
    }
}