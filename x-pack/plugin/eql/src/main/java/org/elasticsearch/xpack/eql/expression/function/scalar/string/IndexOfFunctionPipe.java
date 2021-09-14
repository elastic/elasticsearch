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

public class IndexOfFunctionPipe extends Pipe {

    private final Pipe input, substring, start;
    private final boolean caseInsensitive;

    public IndexOfFunctionPipe(Source source, Expression expression, Pipe input, Pipe substring, Pipe start, boolean caseInsensitive) {
        super(source, expression, Arrays.asList(input, substring, start));
        this.input = input;
        this.substring = substring;
        this.start = start;
        this.caseInsensitive = caseInsensitive;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newInput = input.resolveAttributes(resolver);
        Pipe newSubstring = substring.resolveAttributes(resolver);
        Pipe newStart = start.resolveAttributes(resolver);
        if (newInput == input && newSubstring == substring && newStart == start) {
            return this;
        }
        return replaceChildren(newInput, newSubstring, newStart);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return input.supportedByAggsOnlyQuery() && substring.supportedByAggsOnlyQuery() && start.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return input.resolved() && substring.resolved() && start.resolved();
    }

    protected IndexOfFunctionPipe replaceChildren(Pipe newInput, Pipe newSubstring, Pipe newStart) {
        return new IndexOfFunctionPipe(source(), expression(), newInput, newSubstring, newStart, caseInsensitive);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
        substring.collectFields(sourceBuilder);
        start.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<IndexOfFunctionPipe> info() {
        return NodeInfo.create(this, IndexOfFunctionPipe::new, expression(), input, substring, start, caseInsensitive);
    }

    @Override
    public IndexOfFunctionProcessor asProcessor() {
        return new IndexOfFunctionProcessor(input.asProcessor(), substring.asProcessor(), start.asProcessor(), caseInsensitive);
    }

    public Pipe input() {
        return input;
    }

    public Pipe substring() {
        return substring;
    }

    public Pipe start() {
        return start;
    }

    protected boolean isCaseInsensitive() {
        return caseInsensitive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input, substring, start, caseInsensitive);
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
        return Objects.equals(input(), other.input())
            && Objects.equals(substring(), other.substring())
            && Objects.equals(start(), other.start())
            && Objects.equals(isCaseInsensitive(), other.isCaseInsensitive());
    }
}
