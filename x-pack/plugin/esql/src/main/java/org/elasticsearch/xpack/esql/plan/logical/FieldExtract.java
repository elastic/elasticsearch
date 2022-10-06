/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FieldExtract extends UnaryPlan {

    private final EsIndex index;
    private final List<Attribute> attrs;
    private final List<Attribute> esQueryAttrs;

    public FieldExtract(Source source, LogicalPlan child, EsIndex index, List<Attribute> attrs, List<Attribute> esQueryAttrs) {
        super(source, child);
        this.index = index;
        this.attrs = attrs;
        this.esQueryAttrs = esQueryAttrs;
    }

    public FieldExtract(Source source, LogicalPlan child, EsIndex index, List<Attribute> esQueryAttrs) {
        this(source, child, index, flatten(source, index.mapping()), esQueryAttrs);
    }

    @Override
    protected NodeInfo<FieldExtract> info() {
        return NodeInfo.create(this, FieldExtract::new, child(), index, attrs, esQueryAttrs);
    }

    private static List<Attribute> flatten(Source source, Map<String, EsField> mapping) {
        return flatten(source, mapping, null);
    }

    private static List<Attribute> flatten(Source source, Map<String, EsField> mapping, FieldAttribute parent) {
        List<Attribute> list = new ArrayList<>();

        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String name = entry.getKey();
            EsField t = entry.getValue();

            if (t != null) {
                FieldAttribute f = new FieldAttribute(source, parent, parent != null ? parent.name() + "." + name : name, t);
                list.add(f);
                // object or nested
                if (t.getProperties().isEmpty() == false) {
                    list.addAll(flatten(source, t.getProperties(), f));
                }
            }
        }
        return list;
    }

    public EsIndex index() {
        return index;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new FieldExtract(source(), newChild, index, attrs, esQueryAttrs);
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
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, attrs, esQueryAttrs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FieldExtract other = (FieldExtract) obj;
        return Objects.equals(index, other.index) && Objects.equals(attrs, other.attrs) && Objects.equals(esQueryAttrs, other.esQueryAttrs);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + index + "]" + NodeUtils.limitedToString(attrs);
    }
}
