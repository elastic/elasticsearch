/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class Enrich extends UnaryPlan {
    private final String policyName;
    private final NamedExpression matchField;

    public Enrich(Source source, LogicalPlan child, String policyName, NamedExpression matchField) {
        super(source, child);
        this.policyName = policyName;
        this.matchField = matchField;
    }

    public String policyName() {
        return policyName;
    }

    public NamedExpression matchField() {
        return matchField;
    }

    @Override
    public boolean expressionsResolved() {
        return matchField.resolved();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Enrich(source(), newChild, policyName, matchField);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Enrich::new, child(), policyName, matchField);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Enrich enrich = (Enrich) o;
        return Objects.equals(policyName, enrich.policyName) && Objects.equals(matchField, enrich.matchField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), policyName, matchField);
    }
}
