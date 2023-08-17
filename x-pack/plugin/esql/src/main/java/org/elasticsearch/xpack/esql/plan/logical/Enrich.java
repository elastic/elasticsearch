/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolution;
import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.EmptyAttribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Enrich extends UnaryPlan {
    private final Expression policyName;
    private final NamedExpression matchField;
    private final EnrichPolicyResolution policy;
    private List<NamedExpression> enrichFields;
    private List<Attribute> output;

    public Enrich(
        Source source,
        LogicalPlan child,
        Expression policyName,
        NamedExpression matchField,
        EnrichPolicyResolution policy,
        List<NamedExpression> enrichFields
    ) {
        super(source, child);
        this.policyName = policyName;
        this.matchField = matchField;
        this.policy = policy;
        this.enrichFields = enrichFields;
    }

    public NamedExpression matchField() {
        return matchField;
    }

    public List<NamedExpression> enrichFields() {
        return enrichFields;
    }

    public EnrichPolicyResolution policy() {
        return policy;
    }

    public Expression policyName() {
        return policyName;
    }

    @Override
    public boolean expressionsResolved() {
        return policyName.resolved()
            && matchField instanceof EmptyAttribute == false // matchField not defined in the query, needs to be resolved from the policy
            && matchField.resolved()
            && Resolvables.resolved(enrichFields());
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Enrich(source(), newChild, policyName, matchField, policy, enrichFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Enrich::new, child(), policyName, matchField, policy, enrichFields);
    }

    @Override
    public List<Attribute> output() {
        if (enrichFields == null) {
            return child().output();
        }
        if (this.output == null) {
            this.output = mergeOutputAttributes(enrichFields(), child().output());
        }
        return output;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Enrich enrich = (Enrich) o;
        return Objects.equals(policyName, enrich.policyName)
            && Objects.equals(matchField, enrich.matchField)
            && Objects.equals(policy, enrich.policy)
            && Objects.equals(enrichFields, enrich.enrichFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), policyName, matchField, policy, enrichFields);
    }
}
