/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.processor.ChainingProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

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
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
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
    public void collectFields(SqlSourceBuilder sourceBuilder) {
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