/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;

public class Join extends BinaryPlan {

    private final JoinType type;
    private final Expression condition;

    public enum JoinType {
        INNER,
        LEFT, // OUTER
        RIGHT, // OUTER
        FULL, // OUTER
        IMPLICIT,
    }

    public Join(Source source, LogicalPlan left, LogicalPlan right, JoinType type, Expression condition) {
        super(source, left, right);
        this.type = type;
        this.condition = condition;
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(this, Join::new, left(), right(), type, condition);
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new Join(source(), newChildren.get(0), newChildren.get(1), type, condition);
    }

    public JoinType type() {
        return type;
    }

    public Expression condition() {
        return condition;
    }

    @Override
    public List<Attribute> output() {
        switch (type) {
            case LEFT:
                // right side can be null
                return combine(left().output(), makeNullable(right().output()));
            case RIGHT:
                // left side can be null
                return combine(makeNullable(left().output()), right().output());
            case FULL:
                // both sides can be null
                return combine(makeNullable(left().output()), makeNullable(right().output()));
            // INNER
            default:
                return combine(left().output(), right().output());
        }
    }

    private static List<Attribute> makeNullable(List<Attribute> output) {
        return output.stream()
                .map(a -> a.withNullability(Nullability.TRUE))
                .collect(toList());
    }

    @Override
    public boolean expressionsResolved() {
        return condition == null || condition.resolved();
    }

    public boolean duplicatesResolved() {
        return left().outputSet().intersect(right().outputSet()).isEmpty();
    }

    @Override
    public boolean resolved() {
        // resolve the join if
        // - the children are resolved
        // - there are no conflicts in output
        // - the condition (if present) is resolved to a boolean
        return childrenResolved() &&
                duplicatesResolved() &&
                expressionsResolved() &&
                (condition == null || DataTypes.BOOLEAN == condition.dataType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, condition, left(), right());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Join other = (Join) obj;

        return Objects.equals(type, other.type)
                && Objects.equals(condition, other.condition)
                && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}
