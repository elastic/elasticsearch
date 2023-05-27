/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class EsRelation extends LeafPlan {

    private final EsIndex index;
    private final List<Attribute> attrs;
    private final boolean frozen;

    public EsRelation(Source source, EsIndex index, boolean frozen) {
        this(source, index, flatten(source, index.mapping()), frozen);
    }

    public EsRelation(Source source, EsIndex index, List<Attribute> attributes) {
        this(source, index, attributes, false);
    }

    private EsRelation(Source source, EsIndex index, List<Attribute> attributes, boolean frozen) {
        super(source);
        this.index = index;
        this.attrs = attributes;
        this.frozen = frozen;
    }

    @Override
    protected NodeInfo<EsRelation> info() {
        return NodeInfo.create(this, EsRelation::new, index, frozen);
    }

    private static List<Attribute> flatten(Source source, Map<String, EsField> mapping) {
        return flatten(source, mapping, null);
    }

    private static List<Attribute> flatten(Source source, Map<String, EsField> mapping, FieldAttribute parent) {
        List<Attribute> list = new ArrayList<>();

        for (Entry<String, EsField> entry : mapping.entrySet()) {
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

    public boolean frozen() {
        return frozen;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, frozen);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsRelation other = (EsRelation) obj;
        return Objects.equals(index, other.index) && frozen == other.frozen;
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + index + "]" + NodeUtils.limitedToString(attrs);
    }
}
