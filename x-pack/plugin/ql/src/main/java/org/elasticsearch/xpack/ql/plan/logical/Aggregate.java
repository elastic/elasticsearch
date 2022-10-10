/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class Aggregate extends UnaryPlan {

    private final List<Expression> groupings;
    private final List<? extends NamedExpression> aggregates;

    private final Mode mode;

    public enum Mode {
        SINGLE,
        PARTIAL, // maps raw inputs to intermediate outputs
        FINAL, // maps intermediate inputs to final outputs
    }

    public Aggregate(Source source, LogicalPlan child, List<Expression> groupings, List<? extends NamedExpression> aggregates) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
        this.mode = Mode.SINGLE;
    }

    public Aggregate(Source source, LogicalPlan child, List<Expression> groupings, List<? extends NamedExpression> aggregates, Mode mode) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
        this.mode = mode;
    }

    @Override
    protected NodeInfo<Aggregate> info() {
        return NodeInfo.create(this, Aggregate::new, child(), groupings, aggregates, mode);
    }

    @Override
    public Aggregate replaceChild(LogicalPlan newChild) {
        return new Aggregate(source(), newChild, groupings, aggregates, mode);
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    public Mode getMode() {
        return mode;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(groupings) && Resolvables.resolved(aggregates);
    }

    @Override
    public List<Attribute> output() {
        return Expressions.asAttributes(aggregates);
    }

    @Override
    public boolean singleNode() {
        if (mode != Mode.PARTIAL) {
            return true;
        }
        return child().singleNode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, mode, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Aggregate other = (Aggregate) obj;
        return Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(mode, other.mode)
            && Objects.equals(child(), other.child());
    }
}
