/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;

/**
 * Marker node used inside PromQL as the children of all selectors. When embedded inside ESQL, the relationship can be replaced with the
 * subplan.
 */
public class PlaceholderRelation extends LeafPlan {

    public static final LogicalPlan INSTANCE = new PlaceholderRelation(Source.EMPTY);

    public PlaceholderRelation(Source source) {
        super(source);
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
        return PlaceholderRelation.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PlaceholderRelation;
    }

    @Override
    public List<Attribute> output() {
        return List.of();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("does not support serialization");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("does not support serialization");
    }
}
