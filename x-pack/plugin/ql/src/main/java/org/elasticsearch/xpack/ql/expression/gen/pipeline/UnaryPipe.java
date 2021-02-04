/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.pipeline;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.processor.ChainingProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public final class UnaryPipe extends Pipe {

    private final Pipe child;
    private final Processor action;

    public UnaryPipe(Source source, Expression expression, Pipe child, Processor action) {
        super(source, expression, singletonList(child));
        this.child = child;
        this.action = action;
    }

    @Override
    protected NodeInfo<UnaryPipe> info() {
        return NodeInfo.create(this, UnaryPipe::new, expression(), child, action);
    }

    @Override
    public Pipe replaceChildren(List<Pipe> newChildren) {
        return new UnaryPipe(source(), expression(), newChildren.get(0), action);
    }

    public Pipe child() {
        return child;
    }

    public Processor action() {
        return action;
    }

    @Override
    public boolean resolved() {
        return child.resolved();
    }

    @Override
    public Processor asProcessor() {
        return new ChainingProcessor(child.asProcessor(), action);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return child.supportedByAggsOnlyQuery();
    }

    @Override
    public Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newChild = child.resolveAttributes(resolver);
        if (newChild == child) {
            return this;
        }
        return new UnaryPipe(source(), expression(), newChild, action);
    }

    @Override
    public void collectFields(QlSourceBuilder sourceBuilder) {
        child.collectFields(sourceBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression(), child, action);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnaryPipe other = (UnaryPipe) obj;
        return Objects.equals(action, other.action)
                && Objects.equals(child, other.child)
                && Objects.equals(expression(), other.expression());
    }
}
