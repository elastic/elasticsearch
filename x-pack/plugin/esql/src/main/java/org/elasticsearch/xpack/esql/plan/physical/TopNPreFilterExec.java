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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

public class TopNPreFilterExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "TopNPreFilterExec",
        TopNPreFilterExec::new
    );

    private final Attribute key;
    private final Expression limit;
    private final boolean asc;
    private final boolean nullsFirst;

    public TopNPreFilterExec(Source source, PhysicalPlan child, Attribute key, Expression limit, boolean asc, boolean nullsFirst) {
        super(source, child);
        this.key = key;
        this.limit = limit;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
    }

    private TopNPreFilterExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Expression.class),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(key);
        out.writeNamedWriteable(limit);
        out.writeBoolean(asc);
        out.writeBoolean(nullsFirst);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Attribute key() {
        return key;
    }

    public Expression limit() {
        return limit;
    }

    public boolean asc() {
        return asc;
    }

    public boolean nullsFirst() {
        return nullsFirst;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new TopNPreFilterExec(source(), newChild, key, limit, asc, nullsFirst);
    }

    @Override
    protected NodeInfo<TopNPreFilterExec> info() {
        return NodeInfo.create(this, TopNPreFilterExec::new, child(), key, limit, asc, nullsFirst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), key, limit, asc, nullsFirst);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TopNPreFilterExec other = (TopNPreFilterExec) obj;
        return child().equals(other.child())
            && key.equals(other.key)
            && limit.equals(other.limit)
            && asc == other.asc
            && nullsFirst == other.nullsFirst;
    }
}
