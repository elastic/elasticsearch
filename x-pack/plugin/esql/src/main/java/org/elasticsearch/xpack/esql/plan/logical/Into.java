/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * Terminal command that writes the query's rows into {@link #destination()}. Stripped by {@code EsqlSession} right after
 * parsing, so it is never analyzed, optimized, or serialized.
 */
public class Into extends UnaryPlan implements TelemetryAware {

    private final String destination;

    public Into(Source source, LogicalPlan child, String destination) {
        super(source, child);
        this.destination = Objects.requireNonNull(destination);
    }

    public String destination() {
        return destination;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public Into replaceChild(LogicalPlan newChild) {
        return new Into(source(), newChild, destination);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Into::new, child(), destination);
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), destination);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && destination.equals(((Into) obj).destination);
    }
}
