/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@Experimental
public class FieldExtractExec extends UnaryExec {

    private final Collection<Attribute> attributesToExtract;
    private final List<Attribute> sourceAttribute;

    public FieldExtractExec(Source source, PhysicalPlan child, Collection<Attribute> attributesToExtract) {
        super(source, child);
        this.attributesToExtract = attributesToExtract;
        this.sourceAttribute = extractSourceAttributesFrom(child);

        // TODO: this can be moved into the physical verifier
        if (sourceAttribute.isEmpty()) {
            throw new QlIllegalArgumentException(
                "Need to add field extractor for [{}] but cannot detect source attributes from node [{}]",
                Expressions.names(attributesToExtract),
                child
            );
        }
    }

    public static List<Attribute> extractSourceAttributesFrom(PhysicalPlan plan) {
        var list = new ArrayList<Attribute>(EsQueryExec.NAMES_SET.size());
        plan.outputSet().forEach(e -> {
            if (EsQueryExec.isSourceAttribute(e)) {
                list.add(e);
            }
        });
        // the physical plan expected things sorted out alphabetically
        Collections.sort(list, Comparator.comparing(Attribute::name));
        return list;
    }

    @Override
    protected NodeInfo<FieldExtractExec> info() {
        return NodeInfo.create(this, FieldExtractExec::new, child(), attributesToExtract);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FieldExtractExec(source(), newChild, attributesToExtract);
    }

    public Collection<Attribute> attributesToExtract() {
        return attributesToExtract;
    }

    public List<Attribute> sourceAttributes() {
        return sourceAttribute;
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> output = new ArrayList<>(child().output());
        output.addAll(attributesToExtract);
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributesToExtract, child());
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
        return Objects.equals(attributesToExtract, other.attributesToExtract) && Objects.equals(child(), other.child());
    }

    @Override
    public String nodeString() {
        return nodeName() + NodeUtils.limitedToString(attributesToExtract);
    }
}
