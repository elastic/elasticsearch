/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;

public class FieldExtractExec extends UnaryExec implements EstimatesRowSize {
    private final List<Attribute> attributesToExtract;
    private final Attribute sourceAttribute;
    // attributes to extract as doc values
    private final Set<Attribute> docValuesAttributes;

    private List<Attribute> lazyOutput;

    public FieldExtractExec(Source source, PhysicalPlan child, List<Attribute> attributesToExtract) {
        this(source, child, attributesToExtract, emptySet());
    }

    public FieldExtractExec(Source source, PhysicalPlan child, List<Attribute> attributesToExtract, Set<Attribute> docValuesAttributes) {
        super(source, child);
        this.attributesToExtract = attributesToExtract;
        this.sourceAttribute = extractSourceAttributesFrom(child);
        this.docValuesAttributes = docValuesAttributes;
    }

    public static Attribute extractSourceAttributesFrom(PhysicalPlan plan) {
        for (Attribute attribute : plan.outputSet()) {
            if (EsQueryExec.isSourceAttribute(attribute)) {
                return attribute;
            }
        }
        return null;
    }

    @Override
    protected NodeInfo<FieldExtractExec> info() {
        return NodeInfo.create(this, FieldExtractExec::new, child(), attributesToExtract, docValuesAttributes);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FieldExtractExec(source(), newChild, attributesToExtract, docValuesAttributes);
    }

    public List<Attribute> attributesToExtract() {
        return attributesToExtract;
    }

    public Attribute sourceAttribute() {
        return sourceAttribute;
    }

    public Collection<Attribute> docValuesAttributes() {
        return docValuesAttributes;
    }

    public boolean hasDocValuesAttribute(Attribute attr) {
        return docValuesAttributes.contains(attr);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<Attribute> childOutput = child().output();
            lazyOutput = new ArrayList<>(childOutput.size() + attributesToExtract.size());
            lazyOutput.addAll(childOutput);
            lazyOutput.addAll(attributesToExtract);
        }

        return lazyOutput;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(true, attributesToExtract);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributesToExtract, docValuesAttributes, child());
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
            && Objects.equals(docValuesAttributes, other.docValuesAttributes)
            && Objects.equals(child(), other.child());
    }

    @Override
    public String nodeString() {
        return nodeName() + NodeUtils.limitedToString(attributesToExtract) + "<" + NodeUtils.limitedToString(docValuesAttributes) + ">";
    }

}
