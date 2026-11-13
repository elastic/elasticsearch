/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.into.IntoStats;

import java.util.List;
import java.util.Objects;

/**
 * Coordinator-only root placed on top of the final physical plan for an {@code INTO} query, whose {@link #output()}
 * is the {@link IntoStats} schema.
 */
public class IntoExec extends UnaryExec {
    private final String destination;

    public IntoExec(Source source, PhysicalPlan child, String destination) {
        super(source, child);
        this.destination = Objects.requireNonNull(destination);
    }

    public String destination() {
        return destination;
    }

    @Override
    public List<Attribute> output() {
        return IntoStats.ATTRIBUTES;
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
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new IntoExec(source(), newChild, destination);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, IntoExec::new, child(), destination);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), destination);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && destination.equals(((IntoExec) obj).destination);
    }
}
