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

public class ReplaceFunctionPipe extends Pipe {

    private final Pipe input, pattern, replacement;

    public ReplaceFunctionPipe(Source source, Expression expression, Pipe input, Pipe pattern, Pipe replacement) {
        super(source, expression, Arrays.asList(input, pattern, replacement));
        this.input = input;
        this.pattern = pattern;
        this.replacement = replacement;
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
        Pipe newInput = input.resolveAttributes(resolver);
        Pipe newPattern = pattern.resolveAttributes(resolver);
        Pipe newReplacement = replacement.resolveAttributes(resolver);
        if (newInput == input && newPattern == pattern && newReplacement == replacement) {
            return this;
        }
        return replaceChildren(newInput, newPattern, newReplacement);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return input.supportedByAggsOnlyQuery() && pattern.supportedByAggsOnlyQuery() && replacement.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return input.resolved() && pattern.resolved() && replacement.resolved();
    }
    
    protected Pipe replaceChildren(Pipe newInput, Pipe newPattern, Pipe newReplacement) {
        return new ReplaceFunctionPipe(source(), expression(), newInput, newPattern, newReplacement);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
        pattern.collectFields(sourceBuilder);
        replacement.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<ReplaceFunctionPipe> info() {
        return NodeInfo.create(this, ReplaceFunctionPipe::new, expression(), input, pattern, replacement);
    }

    @Override
    public ReplaceFunctionProcessor asProcessor() {
        return new ReplaceFunctionProcessor(input.asProcessor(), pattern.asProcessor(), replacement.asProcessor());
    }
    
    public Pipe input() {
        return input;
    }
    
    public Pipe pattern() {
        return pattern;
    }
    
    public Pipe replacement() {
        return replacement;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(input, pattern, replacement);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ReplaceFunctionPipe other = (ReplaceFunctionPipe) obj;
        return Objects.equals(input, other.input)
                && Objects.equals(pattern, other.pattern)
                && Objects.equals(replacement, other.replacement);
    }
}
