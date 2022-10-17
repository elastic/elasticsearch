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
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.ql.util.CollectionUtils.mapSize;

@Experimental
public class FieldExtractExec extends UnaryExec {

    private final Set<Attribute> attributesToExtract;
    private final Set<Attribute> sourceAttributes;

    public FieldExtractExec(Source source, PhysicalPlan child, Set<Attribute> attributesToExtract) {
        super(source, child);
        this.attributesToExtract = attributesToExtract;

        var sourceAttr = new LinkedHashSet<Attribute>(mapSize(3));
        child.outputSet().forEach(a -> {
            if (EsQueryExec.isSourceAttribute(a)) {
                sourceAttr.add(a);
            }
        });
        if (sourceAttr.size() != 3) {
            throw new QlIllegalArgumentException(
                "Cannot find source attributes in the input to the source extractor from {}, discovered only {}",
                child.toString(),
                sourceAttr
            );
        }

        this.sourceAttributes = sourceAttr;
    }

    @Override
    protected NodeInfo<FieldExtractExec> info() {
        return NodeInfo.create(this, FieldExtractExec::new, child(), attributesToExtract);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FieldExtractExec(source(), newChild, attributesToExtract);
    }

    public Set<Attribute> attributesToExtract() {
        return attributesToExtract;
    }

    public Set<Attribute> sourceAttributes() {
        return sourceAttributes;
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> output = new ArrayList<>(child().output());
        output.addAll(attributesToExtract);
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributesToExtract);
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
