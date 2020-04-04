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

public class BetweenFunctionPipe extends Pipe {

    private final Pipe source, left, right, greedy, caseSensitive;

    public BetweenFunctionPipe(Source source, Expression expression, Pipe src, Pipe left, Pipe right, Pipe greedy, Pipe caseSensitive) {
        super(source, expression, Arrays.asList(src, left, right, greedy, caseSensitive));
        this.source = src;
        this.left = left;
        this.right = right;
        this.greedy = greedy;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() != 5) {
            throw new IllegalArgumentException("expected [5] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3), newChildren.get(4));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newSource = source.resolveAttributes(resolver);
        Pipe newLeft = left.resolveAttributes(resolver);
        Pipe newRight = right.resolveAttributes(resolver);
        Pipe newGreedy = greedy.resolveAttributes(resolver);
        Pipe newCaseSensitive = caseSensitive.resolveAttributes(resolver);
        if (newSource == source && newLeft == left && newRight == right && newGreedy == greedy && newCaseSensitive == caseSensitive) {
            return this;
        }
        return replaceChildren(newSource, newLeft, newRight, newGreedy, newCaseSensitive);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return source.supportedByAggsOnlyQuery() && left.supportedByAggsOnlyQuery() && right.supportedByAggsOnlyQuery()
                && greedy.supportedByAggsOnlyQuery() && caseSensitive.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return source.resolved() && left.resolved() && right.resolved() && greedy.resolved() && caseSensitive.resolved();
    }

    protected Pipe replaceChildren(Pipe newSource, Pipe newLeft, Pipe newRight, Pipe newGreedy, Pipe newCaseSensitive) {
        return new BetweenFunctionPipe(source(), expression(), newSource, newLeft, newRight, newGreedy, newCaseSensitive);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        source.collectFields(sourceBuilder);
        left.collectFields(sourceBuilder);
        right.collectFields(sourceBuilder);
        greedy.collectFields(sourceBuilder);
        caseSensitive.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<BetweenFunctionPipe> info() {
        return NodeInfo.create(this, BetweenFunctionPipe::new, expression(), source, left, right, greedy, caseSensitive);
    }

    @Override
    public BetweenFunctionProcessor asProcessor() {
        return new BetweenFunctionProcessor(source.asProcessor(), left.asProcessor(), right.asProcessor(),
                greedy.asProcessor(), caseSensitive.asProcessor());
    }

    public Pipe src() {
        return source;
    }

    public Pipe left() {
        return left;
    }

    public Pipe right() {
        return right;
    }

    public Pipe greedy() {
        return greedy;
    }

    public Pipe caseSensitive() {
        return caseSensitive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), left(), right(), greedy(), caseSensitive());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BetweenFunctionPipe other = (BetweenFunctionPipe) obj;
        return Objects.equals(source(), other.source())
                && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right())
                && Objects.equals(greedy(), other.greedy())
                && Objects.equals(caseSensitive(), other.caseSensitive());
    }
}
