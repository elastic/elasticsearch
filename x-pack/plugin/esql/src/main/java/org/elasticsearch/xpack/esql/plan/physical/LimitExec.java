/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

public class LimitExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "LimitExec",
        LimitExec::new
    );

    private final Expression limit;

    public LimitExec(Source source, PhysicalPlan child, Expression limit) {
        super(source, child);
        this.limit = limit;
    }

    private LimitExec(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(PhysicalPlan.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(limit());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends LimitExec> info() {
        return NodeInfo.create(this, LimitExec::new, child(), limit);
    }

    @Override
    public LimitExec replaceChild(PhysicalPlan newChild) {
        return new LimitExec(source(), newChild, limit);
    }

    public Expression limit() {
        return limit;
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

        LimitExec other = (LimitExec) obj;
        return Objects.equals(limit, other.limit) && Objects.equals(child(), other.child());
    }
}
