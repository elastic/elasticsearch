/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.*;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.CoreJoinType.FULL;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.CoreJoinType.LEFT;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.CoreJoinType.RIGHT;

public class Join extends BinaryPlan {

    private final JoinType type;
    private final Expression condition;
    private List<Attribute> lazyOutput;

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
    public Join replaceChildren(List<LogicalPlan> newChildren) {
        return new Join(source(), newChildren.get(0), newChildren.get(1), type, condition);
    }

    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new Join(source(), left, right, type, condition);
    }

    public JoinType type() {
        return type;
    }

    public Expression condition() {
        return condition;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = computeOutput();
        }
        return lazyOutput;
    }

    private List<Attribute> computeOutput() {
        if (type == LEFT) {
            // right side can be null
            return NamedExpressions.mergeOutputAttributes(left().output(), makeNullable(right().output()));
        }
        if (type == RIGHT) {
            // left side can be null
            return NamedExpressions.mergeOutputAttributes(makeNullable(left().output()), right().output());
        }
        if (type == FULL) {
            // both sides can be null
            return NamedExpressions.mergeOutputAttributes(makeNullable(left().output()), makeNullable(right().output()));
        }
        // the rest - INNER, CROSS
        return NamedExpressions.mergeOutputAttributes(left().output(), right().output());
    }

    private static List<Attribute> makeNullable(List<Attribute> output) {
        List<Attribute> out = new ArrayList<>(output.size());
        for (Attribute a : output) {
            out.add(a.withNullability(Nullability.TRUE));
        }
        return out;
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
        return childrenResolved()
            && duplicatesResolved()
            && expressionsResolved()
            && (condition == null || DataTypes.BOOLEAN == condition.dataType());
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
