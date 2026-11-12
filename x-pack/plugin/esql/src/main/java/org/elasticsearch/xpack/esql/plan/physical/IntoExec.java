/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.into.IntoStats;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Coordinator-only root placed on top of the final physical plan for an {@code INTO} query. Its {@link #output()} is
 * the {@link IntoStats} schema, while {@code ComputeService} swaps it for an {@link OutputExec} feeding {@link #writer()}.
 */
public class IntoExec extends UnaryExec {

    private final String destination;
    @Nullable
    private final PageConsumer writer;

    public IntoExec(Source source, PhysicalPlan child, String destination, @Nullable PageConsumer writer) {
        super(source, child);
        this.destination = Objects.requireNonNull(destination);
        this.writer = writer;
    }

    public String destination() {
        return destination;
    }

    @Nullable
    public PageConsumer writer() {
        return writer;
    }

    public IntoExec withWriter(PageConsumer writer) {
        return new IntoExec(source(), child(), destination, writer);
    }

    @Override
    public List<Attribute> output() {
        return IntoStats.ATTRIBUTES;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("INTO runs on the coordinator and is never serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("INTO runs on the coordinator and is never serialized");
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new IntoExec(source(), newChild, destination, writer);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, IntoExec::new, child(), destination, writer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), destination, writer);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IntoExec other = (IntoExec) obj;
        return Objects.equals(child(), other.child()) && destination.equals(other.destination) && Objects.equals(writer, other.writer);
    }
}
