/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.show;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

public class ShowInfo extends LeafPlan {

    private final List<Attribute> attributes;

    public ShowInfo(Source source) {
        super(source);

        attributes = new ArrayList<>();
        for (var name : List.of("version", "date", "hash")) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, KEYWORD));
        }
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    public List<List<Object>> values() {
        List<Object> row = new ArrayList<>(attributes.size());
        row.add(new BytesRef(Build.CURRENT.version()));
        row.add(new BytesRef(Build.CURRENT.date()));
        row.add(new BytesRef(Build.CURRENT.hash()));
        return List.of(row);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj != null && getClass() == obj.getClass();
    }
}
