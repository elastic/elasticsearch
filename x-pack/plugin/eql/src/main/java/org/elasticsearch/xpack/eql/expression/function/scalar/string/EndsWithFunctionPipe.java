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

public class EndsWithFunctionPipe extends Pipe {

    private final Pipe input, pattern;
    private final boolean isCaseSensitive;

    public EndsWithFunctionPipe(Source source, Expression expression, Pipe input, Pipe pattern, boolean isCaseSensitive) {
        super(source, expression, Arrays.asList(input, pattern));
        this.input = input;
        this.pattern = pattern;
        this.isCaseSensitive = isCaseSensitive;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
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
        return new EndsWithFunctionPipe(source(), expression(), newInput, newPattern, isCaseSensitive);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
        pattern.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<EndsWithFunctionPipe> info() {
        return NodeInfo.create(this, EndsWithFunctionPipe::new, expression(), input, pattern, isCaseSensitive);
    }

    @Override
    public EndsWithFunctionProcessor asProcessor() {
        return new EndsWithFunctionProcessor(input.asProcessor(), pattern.asProcessor(), isCaseSensitive);
    }
    
    public Pipe input() {
        return input;
    }

    public Pipe pattern() {
        return pattern;
    }

    protected boolean isCaseSensitive() {
        return isCaseSensitive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input, pattern, isCaseSensitive);
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
                && Objects.equals(isCaseSensitive(), other.isCaseSensitive());
    }
}
