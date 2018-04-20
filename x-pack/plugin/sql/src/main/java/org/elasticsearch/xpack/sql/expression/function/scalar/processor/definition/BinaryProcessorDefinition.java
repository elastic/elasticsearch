/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Arrays;
import java.util.List;

public abstract class BinaryProcessorDefinition extends ProcessorDefinition {

    private final ProcessorDefinition left, right;

    public BinaryProcessorDefinition(Location location, Expression expression, ProcessorDefinition left, ProcessorDefinition right) {
        super(location, expression, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    @Override
    public final ProcessorDefinition replaceChildren(List<ProcessorDefinition> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1));
    }

    public ProcessorDefinition left() {
        return left;
    }

    public ProcessorDefinition right() {
        return right;
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return left.supportedByAggsOnlyQuery() && right.supportedByAggsOnlyQuery();
    }

    @Override
    public final ProcessorDefinition resolveAttributes(AttributeResolver resolver) {
        ProcessorDefinition newLeft = left.resolveAttributes(resolver);
        ProcessorDefinition newRight = right.resolveAttributes(resolver);
        if (newLeft == left && newRight == right) {
            return this;
        }
        return replaceChildren(newLeft, newRight);
    }

    /**
     * Build a copy of this object with new left and right children. Used by
     * {@link #resolveAttributes(AttributeResolver)}.
     */
    protected abstract BinaryProcessorDefinition replaceChildren(ProcessorDefinition left, ProcessorDefinition right);

    @Override
    public boolean resolved() {
        return left().resolved() && right().resolved();
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        left.collectFields(sourceBuilder);
        right.collectFields(sourceBuilder);
    }
}
