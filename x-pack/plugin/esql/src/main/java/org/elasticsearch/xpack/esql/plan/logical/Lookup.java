/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Looks up values from the associated {@code tables}.
 * The class is used only during the analysis phased, after which it is replaced by a {@code Join}.
 */
public class Lookup extends UnaryPlan {
    private final Expression tableName;
    private final List<NamedExpression> matchFields;
    // initialized during the analysis phase for output and validation
    // afterward, it is converted into a Join (BinaryPlan) hence why here it is not a child
    private final LocalRelation localRelation;
    private List<Attribute> lazyOutput;

    public Lookup(Source source, LogicalPlan child, Expression tableName, List<NamedExpression> matchFields) {
        this(source, child, tableName, matchFields, null);
    }

    public Lookup(Source source, LogicalPlan child, Expression tableName, List<NamedExpression> matchFields, LocalRelation localRelation) {
        super(source, child);
        this.tableName = tableName;
        this.matchFields = matchFields;
        this.localRelation = localRelation;
    }

    public Expression tableName() {
        return tableName;
    }

    public List<NamedExpression> matchFields() {
        return matchFields;
    }

    public LocalRelation localRelation() {
        return localRelation;
    }

    @Override
    public boolean expressionsResolved() {
        return tableName.resolved() && Resolvables.resolved(matchFields);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Lookup(source(), newChild, tableName, matchFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Lookup::new, child(), tableName, matchFields);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<? extends NamedExpression> rightSide = localRelation != null ? localRelation.output() : matchFields;
            lazyOutput = mergeOutputAttributes(child().output(), rightSide);
        }
        return lazyOutput;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        Lookup lookup = (Lookup) o;
        return Objects.equals(tableName, lookup.matchFields)
            && Objects.equals(matchFields, lookup.matchFields)
            && Objects.equals(localRelation, lookup.localRelation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), tableName, matchFields, localRelation);
    }
}
