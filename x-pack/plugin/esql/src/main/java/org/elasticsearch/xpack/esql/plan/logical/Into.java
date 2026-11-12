/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Terminal command that writes the query's rows into {@link #destination()} and returns write statistics instead of
 * the rows. Stripped by {@code EsqlSession} right after parsing, so it is never analyzed, optimized, or serialized.
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

    public static void verifyProducer(LogicalPlan producer) {
        if (producer.anyMatch(p -> p instanceof LeafPlan && p instanceof Row == false)) {
            throw new VerificationException("INTO currently only supports ROW as a source");
        }
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public boolean expressionsResolved() {
        return child().expressionsResolved();
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
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("INTO is resolved on the coordinator and never serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("INTO is resolved on the coordinator and never serialized");
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), destination);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Into other = (Into) obj;
        return Objects.equals(child(), other.child()) && destination.equals(other.destination);
    }
}
