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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

/**
 * An approximate filter that, on a best-effort basis, filters out rows whose {@code key} can't be among the top {@code limit} keys,
 * based on the keys it has seen so far. It never drops a row that makes it into the final top {@code limit}, but it may keep rows
 * outside of it. For correctness, it must be followed by an exact {@link TopN} on the same key, which discards those extra,
 * possibly incomplete, rows.
 *
 * @see org.elasticsearch.xpack.esql.optimizer.rules.logical.AddTopNPreFilterToAggregate
 */
public final class TopNPreFilter extends UnaryPlan implements SortPreserving {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "TopNPreFilter",
        TopNPreFilter::new
    );

    private final Attribute key;
    private final Expression limit;
    private final boolean asc;
    private final boolean nullsFirst;

    public TopNPreFilter(Source source, LogicalPlan child, Attribute key, Expression limit, boolean asc, boolean nullsFirst) {
        super(source, child);
        this.key = key;
        this.limit = limit;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
    }

    private TopNPreFilter(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Expression.class),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
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
    public boolean expressionsResolved() {
        return key.resolved() && limit.resolved();
    }

    @Override
    public TopNPreFilter replaceChild(LogicalPlan newChild) {
        return new TopNPreFilter(source(), newChild, key, limit, asc, nullsFirst);
    }

    @Override
    protected NodeInfo<TopNPreFilter> info() {
        return NodeInfo.create(this, TopNPreFilter::new, child(), key, limit, asc, nullsFirst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), key, limit, asc, nullsFirst);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TopNPreFilter other = (TopNPreFilter) obj;
        return child().equals(other.child())
            && key.equals(other.key)
            && limit.equals(other.limit)
            && asc == other.asc
            && nullsFirst == other.nullsFirst;
    }
}
