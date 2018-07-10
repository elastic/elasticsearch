/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ChainingProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public final class UnaryProcessorDefinition extends ProcessorDefinition {

    private final ProcessorDefinition child;
    private final Processor action;

    public UnaryProcessorDefinition(Location location, Expression expression, ProcessorDefinition child, Processor action) {
        super(location, expression, singletonList(child));
        this.child = child;
        this.action = action;
    }

    @Override
    protected NodeInfo<UnaryProcessorDefinition> info() {
        return NodeInfo.create(this, UnaryProcessorDefinition::new, expression(), child, action);
    }

    @Override
    public ProcessorDefinition replaceChildren(List<ProcessorDefinition> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new UnaryProcessorDefinition(location(), expression(), newChildren.get(0), action);
    }

    public ProcessorDefinition child() {
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
    public ProcessorDefinition resolveAttributes(AttributeResolver resolver) {
        ProcessorDefinition newChild = child.resolveAttributes(resolver);
        if (newChild == child) {
            return this;
        }
        return new UnaryProcessorDefinition(location(), expression(), newChild, action);
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

        UnaryProcessorDefinition other = (UnaryProcessorDefinition) obj;
        return Objects.equals(action, other.action)
                && Objects.equals(child, other.child)
                && Objects.equals(expression(), other.expression());
    }
}
