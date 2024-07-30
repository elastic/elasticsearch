/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class EsRelation extends LeafPlan {

    private final EsIndex index;
    private final List<Attribute> attrs;
    private final boolean frozen;
    private final IndexMode indexMode;

    public EsRelation(Source source, EsIndex index, IndexMode indexMode, boolean frozen) {
        this(source, index, flatten(source, index.mapping()), indexMode, frozen);
    }

    public EsRelation(Source source, EsIndex index, List<Attribute> attributes, IndexMode indexMode) {
        this(source, index, attributes, indexMode, false);
    }

    public EsRelation(Source source, EsIndex index, List<Attribute> attributes, IndexMode indexMode, boolean frozen) {
        super(source);
        this.index = index;
        this.attrs = attributes;
        this.indexMode = indexMode;
        this.frozen = frozen;
    }

    @Override
    protected NodeInfo<EsRelation> info() {
        return NodeInfo.create(this, EsRelation::new, index, attrs, indexMode, frozen);
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

    public IndexMode indexMode() {
        return indexMode;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    @Override
    public boolean expressionsResolved() {
        // For unresolved expressions to exist in EsRelation is fine, as long as they are not used in later operations
        // This allows for them to be converted to null@unsupported fields in final output, an important feature of ES|QL
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, indexMode, frozen, attrs);
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
        return Objects.equals(index, other.index)
            && indexMode == other.indexMode()
            && frozen == other.frozen
            && Objects.equals(attrs, other.attrs);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + index + "]" + NodeUtils.limitedToString(attrs);
    }
}
