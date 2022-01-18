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

public class BetweenFunctionPipe extends Pipe {

    private final Pipe input, left, right, greedy;
    private final boolean caseInsensitive;

    public BetweenFunctionPipe(
        Source source,
        Expression expression,
        Pipe input,
        Pipe left,
        Pipe right,
        Pipe greedy,
        boolean caseInsensitive
    ) {
        super(source, expression, Arrays.asList(input, left, right, greedy));
        this.input = input;
        this.left = left;
        this.right = right;
        this.greedy = greedy;
        this.caseInsensitive = caseInsensitive;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newInput = input.resolveAttributes(resolver);
        Pipe newLeft = left.resolveAttributes(resolver);
        Pipe newRight = right.resolveAttributes(resolver);
        Pipe newGreedy = greedy.resolveAttributes(resolver);
        if (newInput == input && newLeft == left && newRight == right && newGreedy == greedy) {
            return this;
        }
        return replaceChildren(newInput, newLeft, newRight, newGreedy);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return input.supportedByAggsOnlyQuery()
            && left.supportedByAggsOnlyQuery()
            && right.supportedByAggsOnlyQuery()
            && greedy.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return input.resolved() && left.resolved() && right.resolved() && greedy.resolved();
    }

    protected Pipe replaceChildren(Pipe newInput, Pipe newLeft, Pipe newRight, Pipe newGreedy) {
        return new BetweenFunctionPipe(source(), expression(), newInput, newLeft, newRight, newGreedy, caseInsensitive);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
        left.collectFields(sourceBuilder);
        right.collectFields(sourceBuilder);
        greedy.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<BetweenFunctionPipe> info() {
        return NodeInfo.create(this, BetweenFunctionPipe::new, expression(), input, left, right, greedy, caseInsensitive);
    }

    @Override
    public BetweenFunctionProcessor asProcessor() {
        return new BetweenFunctionProcessor(
            input.asProcessor(),
            left.asProcessor(),
            right.asProcessor(),
            greedy.asProcessor(),
            caseInsensitive
        );
    }

    public Pipe input() {
        return input;
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

    public boolean isCaseInsensitive() {
        return caseInsensitive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input(), left(), right(), greedy(), caseInsensitive);
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
        return Objects.equals(input(), other.input())
            && Objects.equals(left(), other.left())
            && Objects.equals(right(), other.right())
            && Objects.equals(greedy(), other.greedy())
            && Objects.equals(caseInsensitive, other.caseInsensitive);
    }
}
