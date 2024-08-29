/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

public class Limit extends UnaryPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Limit", Limit::new);

    private final Expression limit;

    public Limit(Source source, Expression limit, LogicalPlan child) {
        super(source, child);
        this.limit = limit;
    }

    private Limit(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(LogicalPlan.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(limit());
        out.writeNamedWriteable(child());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Limit> info() {
        return NodeInfo.create(this, Limit::new, limit, child());
    }

    @Override
    public Limit replaceChild(LogicalPlan newChild) {
        return new Limit(source(), limit, newChild);
    }

    public Expression limit() {
        return limit;
    }

    @Override
    public boolean expressionsResolved() {
        return limit.resolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Limit other = (Limit) obj;

        return Objects.equals(limit, other.limit) && Objects.equals(child(), other.child());
    }
}
