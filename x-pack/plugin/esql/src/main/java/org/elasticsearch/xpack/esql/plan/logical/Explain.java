/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

public class Explain extends LeafPlan implements TelemetryAware {

    public enum Type {
        PARSED,
        ANALYZED
    }

    private final LogicalPlan query;

    public Explain(Source source, LogicalPlan query) {
        super(source);
        this.query = query;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public LogicalPlan query() {
        return query;
    }

    @Override
    public List<Attribute> output() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Explain::new, query);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Explain explain = (Explain) o;
        return Objects.equals(query, explain.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query);
    }
}
