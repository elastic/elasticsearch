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

public class EndsWithFunctionPipe extends Pipe {

    private final Pipe input, pattern;
    private final boolean caseInsensitive;

    public EndsWithFunctionPipe(Source source, Expression expression, Pipe input, Pipe pattern, boolean caseInsensitive) {
        super(source, expression, Arrays.asList(input, pattern));
        this.input = input;
        this.pattern = pattern;
        this.caseInsensitive = caseInsensitive;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newInput = input.resolveAttributes(resolver);
        Pipe newPattern = pattern.resolveAttributes(resolver);
        if (newInput == input && newPattern == pattern) {
            return this;
        }
        return replaceChildren(newInput, newPattern);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return input.supportedByAggsOnlyQuery() && pattern.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return input.resolved() && pattern.resolved();
    }

    protected EndsWithFunctionPipe replaceChildren(Pipe newInput, Pipe newPattern) {
        return new EndsWithFunctionPipe(source(), expression(), newInput, newPattern, caseInsensitive);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
        pattern.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<EndsWithFunctionPipe> info() {
        return NodeInfo.create(this, EndsWithFunctionPipe::new, expression(), input, pattern, caseInsensitive);
    }

    @Override
    public EndsWithFunctionProcessor asProcessor() {
        return new EndsWithFunctionProcessor(input.asProcessor(), pattern.asProcessor(), caseInsensitive);
    }

    public Pipe input() {
        return input;
    }

    public Pipe pattern() {
        return pattern;
    }

    protected boolean isCaseInsensitive() {
        return caseInsensitive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input, pattern, caseInsensitive);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EndsWithFunctionPipe other = (EndsWithFunctionPipe) obj;
        return Objects.equals(input(), other.input())
            && Objects.equals(pattern(), other.pattern())
            && Objects.equals(isCaseInsensitive(), other.isCaseInsensitive());
    }
}
