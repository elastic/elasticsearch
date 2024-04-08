/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class EnrichExec extends UnaryExec implements EstimatesRowSize {

    private final Enrich.Mode mode;
    private final String matchType;
    private final NamedExpression matchField;
    private final String policyName;
    private final String policyMatchField;
    private final Map<String, String> concreteIndices; // cluster -> enrich index
    private final List<NamedExpression> enrichFields;

    /**
     *
     * @param source
     * @param child
     * @param matchField the match field in the source data
     * @param policyName the enrich policy name
     * @param policyMatchField the match field name in the policy
     * @param concreteIndices a map from cluster to concrete enrich indices
     * @param enrichFields the enrich fields
     */
    public EnrichExec(
        Source source,
        PhysicalPlan child,
        Enrich.Mode mode,
        String matchType,
        NamedExpression matchField,
        String policyName,
        String policyMatchField,
        Map<String, String> concreteIndices,
        List<NamedExpression> enrichFields
    ) {
        super(source, child);
        this.mode = mode;
        this.matchType = matchType;
        this.matchField = matchField;
        this.policyName = policyName;
        this.policyMatchField = policyMatchField;
        this.concreteIndices = concreteIndices;
        this.enrichFields = enrichFields;
    }

    @Override
    protected NodeInfo<EnrichExec> info() {
        return NodeInfo.create(
            this,
            EnrichExec::new,
            child(),
            mode,
            matchType,
            matchField,
            policyName,
            policyMatchField,
            concreteIndices,
            enrichFields
        );
    }

    @Override
    public EnrichExec replaceChild(PhysicalPlan newChild) {
        return new EnrichExec(source(), newChild, mode, matchType, matchField, policyName, policyMatchField, concreteIndices, enrichFields);
    }

    public Enrich.Mode mode() {
        return mode;
    }

    public String matchType() {
        return matchType;
    }

    public NamedExpression matchField() {
        return matchField;
    }

    public Map<String, String> concreteIndices() {
        return concreteIndices;
    }

    public List<NamedExpression> enrichFields() {
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
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, enrichFields);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        EnrichExec that = (EnrichExec) o;
        return mode.equals(that.mode)
            && Objects.equals(matchType, that.matchType)
            && Objects.equals(matchField, that.matchField)
            && Objects.equals(policyName, that.policyName)
            && Objects.equals(policyMatchField, that.policyMatchField)
            && Objects.equals(concreteIndices, that.concreteIndices)
            && Objects.equals(enrichFields, that.enrichFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), mode, matchType, matchField, policyName, policyMatchField, concreteIndices, enrichFields);
    }
}
