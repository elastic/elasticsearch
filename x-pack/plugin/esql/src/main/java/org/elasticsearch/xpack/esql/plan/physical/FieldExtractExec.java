/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class FieldExtractExec extends UnaryExec implements EstimatesRowSize {
    private final List<Attribute> attributesToExtract;
    private final Attribute sourceAttribute;
    private final Set<Attribute> preferDocValues;

    public FieldExtractExec(Source source, PhysicalPlan child, List<Attribute> attributesToExtract) {
        this(source, child, attributesToExtract, new HashSet<>());
    }

    public FieldExtractExec(Source source, PhysicalPlan child, List<Attribute> attributesToExtract, Set<Attribute> preferDocValues) {
        super(source, child);
        this.attributesToExtract = attributesToExtract;
        this.sourceAttribute = extractSourceAttributesFrom(child);
        this.preferDocValues = preferDocValues;
    }

    public static Attribute extractSourceAttributesFrom(PhysicalPlan plan) {
        return plan.outputSet().stream().filter(EsQueryExec::isSourceAttribute).findFirst().orElse(null);
    }

    @Override
    protected NodeInfo<FieldExtractExec> info() {
        return NodeInfo.create(this, FieldExtractExec::new, child(), attributesToExtract, preferDocValues);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FieldExtractExec(source(), newChild, attributesToExtract, preferDocValues);
    }

    public List<Attribute> attributesToExtract() {
        return attributesToExtract;
    }

    public Attribute sourceAttribute() {
        return sourceAttribute;
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> output = new ArrayList<>(child().output());
        output.addAll(attributesToExtract);
        return output;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(true, attributesToExtract);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributesToExtract, preferDocValues, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FieldExtractExec other = (FieldExtractExec) obj;
        return Objects.equals(attributesToExtract, other.attributesToExtract)
            && Objects.equals(preferDocValues, other.preferDocValues)
            && Objects.equals(child(), other.child());
    }

    @Override
    public String nodeString() {
        return nodeName() + NodeUtils.limitedToString(attributesToExtract) + NodeUtils.limitedToString(preferDocValues);
    }

    public FieldExtractExec preferDocValues(Attribute attr) {
        Set<Attribute> newForStats = new HashSet<>(preferDocValues);
        newForStats.add(attr);
        return new FieldExtractExec(source(), child(), attributesToExtract, newForStats);
    }

    /**
     * Returns DOC_VALUES if the given attribute should be preferrentially extracted from doc-values.
     */
    public MappedFieldType.FieldExtractPreference extractPreference(Attribute attr) {
        return preferDocValues.contains(attr)
            ? MappedFieldType.FieldExtractPreference.DOC_VALUES
            : MappedFieldType.FieldExtractPreference.NONE;
    }
}
