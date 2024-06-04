/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.show;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.index.IndexResolver;
import org.elasticsearch.xpack.esql.core.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class ShowClusters extends LeafPlan {

    private static final List<Attribute> ATTRIBUTES = Stream.of("name", "type")
        .map(name -> (Attribute) new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD))
        .toList();

    public static final String TYPE_LOCAL = "local";
    public static final String TYPE_REMOTE = "remote";

    public ShowClusters(Source source) {
        super(source);
    }

    @Override
    public List<Attribute> output() {
        return ATTRIBUTES;
    }

    public List<List<Object>> values(IndexResolver indexResolver) {
        Set<String> remotes = indexResolver.remoteClusters();
        List<List<Object>> rows = new ArrayList<>(1 + remotes.size());

        List<Object> row = new ArrayList<>(ATTRIBUTES.size());
        row.add(new BytesRef(indexResolver.clusterName()));
        row.add(new BytesRef(TYPE_LOCAL));
        rows.add(row);

        remotes.forEach(name -> {
            List<Object> r = new ArrayList<>(ATTRIBUTES.size());
            r.add(new BytesRef(name));
            r.add(new BytesRef(TYPE_REMOTE));
            rows.add(r);
        });

        return rows;
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
