/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@code UnaryPlan} is a {@code LogicalPlan} with exactly one child, for example, {@code WHERE x} in a
 * SQL statement is an {@code UnaryPlan}.
 */
public abstract class UnaryPlan extends LogicalPlan {

    private final LogicalPlan child;

    protected UnaryPlan(Source source, LogicalPlan child) {
        super(source, Collections.singletonList(child));
        this.child = child;
    }

    @Override
    public final UnaryPlan replaceChildren(List<LogicalPlan> newChildren) {
        return replaceChild(newChildren.get(0));
    }

    public abstract UnaryPlan replaceChild(LogicalPlan newChild);

    public LogicalPlan child() {
        return child;
    }

    @Override
    public List<Attribute> output() {
        return child.output();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnaryPlan other = (UnaryPlan) obj;

        return Objects.equals(child, other.child);
    }
}
