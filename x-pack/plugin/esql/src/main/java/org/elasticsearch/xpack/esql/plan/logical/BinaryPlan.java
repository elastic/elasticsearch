/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class BinaryPlan extends LogicalPlan {

    private final LogicalPlan left, right;

    protected BinaryPlan(Source source, LogicalPlan left, LogicalPlan right) {
        super(source, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    public LogicalPlan left() {
        return left;
    }

    public LogicalPlan right() {
        return right;
    }

    public abstract AttributeSet leftReferences();

    public abstract AttributeSet rightReferences();

    @Override
    public final BinaryPlan replaceChildren(List<LogicalPlan> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1));
    }

    public final BinaryPlan replaceLeft(LogicalPlan newLeft) {
        return replaceChildren(newLeft, right);
    }

    public final BinaryPlan replaceRight(LogicalPlan newRight) {
        return replaceChildren(left, newRight);
    }

    public abstract BinaryPlan replaceChildren(LogicalPlan left, LogicalPlan right);

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BinaryPlan other = (BinaryPlan) obj;

        return Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

}
