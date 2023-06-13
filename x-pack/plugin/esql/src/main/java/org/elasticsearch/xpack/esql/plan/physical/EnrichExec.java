/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class EnrichExec extends UnaryExec {

    private final NamedExpression matchField;
    private final String policyName;
    private final String policyMatchField;
    private final EsIndex enrichIndex;
    private final List<Attribute> enrichFields;

    /**
     *
     * @param source
     * @param child
     * @param matchField the match field in the source data
     * @param policyName the enrich policy name
     * @param policyMatchField the match field name in the policy
     * @param enrichIndex the enricy policy index (the system index created by the policy execution, not the source index)
     * @param enrichFields the enrich fields
     */
    public EnrichExec(
        Source source,
        PhysicalPlan child,
        NamedExpression matchField,
        String policyName,
        String policyMatchField,
        EsIndex enrichIndex,
        List<Attribute> enrichFields
    ) {
        super(source, child);
        this.matchField = matchField;
        this.policyName = policyName;
        this.policyMatchField = policyMatchField;
        this.enrichIndex = enrichIndex;
        this.enrichFields = enrichFields;
    }

    @Override
    protected NodeInfo<EnrichExec> info() {
        return NodeInfo.create(this, EnrichExec::new, child(), matchField, policyName, policyMatchField, enrichIndex, enrichFields);
    }

    @Override
    public EnrichExec replaceChild(PhysicalPlan newChild) {
        return new EnrichExec(source(), newChild, matchField, policyName, policyMatchField, enrichIndex, enrichFields);
    }

    public NamedExpression matchField() {
        return matchField;
    }

    public EsIndex enrichIndex() {
        return enrichIndex;
    }

    public List<Attribute> enrichFields() {
        return enrichFields;
    }

    public String policyName() {
        return policyName;
    }

    public String policyMatchField() {
        return policyMatchField;
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(enrichFields, child().output());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        EnrichExec that = (EnrichExec) o;
        return Objects.equals(matchField, that.matchField)
            && Objects.equals(policyName, that.policyName)
            && Objects.equals(policyMatchField, that.policyMatchField)
            && Objects.equals(enrichIndex, that.enrichIndex)
            && Objects.equals(enrichFields, that.enrichFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), matchField, policyName, policyMatchField, enrichIndex, enrichFields);
    }
}
