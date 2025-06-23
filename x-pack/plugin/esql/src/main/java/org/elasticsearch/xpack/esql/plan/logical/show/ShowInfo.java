/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.show;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

public class ShowInfo extends LeafPlan implements TelemetryAware {

    private final List<Attribute> attributes;

    public ShowInfo(Source source) {
        super(source);

        attributes = new ArrayList<>();
        for (var name : List.of("version", "date", "hash")) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, KEYWORD));
        }
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    public List<List<Object>> values() {
        List<Object> row = new ArrayList<>(attributes.size());
        row.add(new BytesRef(Build.current().version()));
        row.add(new BytesRef(Build.current().date()));
        row.add(new BytesRef(Build.current().hash()));
        return List.of(row);
    }

    @Override
    public String telemetryLabel() {
        return "SHOW";
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
