/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Experimental
public class FieldExtractExec extends UnaryExec {

    private final EsIndex index;
    private final List<Attribute> attrs;
    private final List<Attribute> esQueryAttrs;

    public FieldExtractExec(Source source, PhysicalPlan child, EsIndex index, List<Attribute> attrs, List<Attribute> esQueryAttrs) {
        super(source, child);
        this.index = index;
        this.attrs = attrs;
        this.esQueryAttrs = esQueryAttrs;
    }

    @Override
    protected NodeInfo<FieldExtractExec> info() {
        return NodeInfo.create(this, FieldExtractExec::new, child(), index, attrs, esQueryAttrs);
    }

    public EsIndex index() {
        return index;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FieldExtractExec(source(), newChild, index, attrs, esQueryAttrs);
    }

    public List<Attribute> getAttrs() {
        return attrs;
    }

    public List<Attribute> getEsQueryAttrs() {
        return esQueryAttrs;
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> output = new ArrayList<>(child().output());
        output.addAll(attrs);
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, attrs, esQueryAttrs, child());
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
        return Objects.equals(index, other.index)
            && Objects.equals(attrs, other.attrs)
            && Objects.equals(esQueryAttrs, other.esQueryAttrs)
            && Objects.equals(child(), other.child());
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + index + "]" + NodeUtils.limitedToString(attrs);
    }
}
