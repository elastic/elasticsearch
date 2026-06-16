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
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TopN extends UnaryPlan implements PipelineBreaker, ExecutesOn {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "TopN", TopN::new);

    private final List<Order> order;
    private final Expression limit;
    /**
     * Local topn is not a pipeline breaker, and is applied only to the local node's data.
     * It should always end up inside a fragment.
     */
    private final transient boolean local;
    /**
     * Set by {@code MarkUnboundedSort} when this TopN represents an unbounded streaming sort.
     * The limit value is {@link Integer#MAX_VALUE} in that case (semantically "no limit"), but this
     * flag — not the value — is the authoritative gate. It is transient (not serialized at the logical
     * level) and is re-established by the optimizer rule on every node.
     */
    private final transient boolean unboundedSort;

    public TopN(Source source, LogicalPlan child, List<Order> order, Expression limit, boolean local) {
        this(source, child, order, limit, local, false);
    }

    private TopN(Source source, LogicalPlan child, List<Order> order, Expression limit, boolean local, boolean unboundedSort) {
        super(source, child);
        this.order = order;
        this.limit = limit;
        this.local = local;
        this.unboundedSort = unboundedSort;
    }

    private TopN(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readCollectionAsList(Order::new),
            in.readNamedWriteable(Expression.class),
            false
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeCollection(order);
        out.writeNamedWriteable(limit);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean expressionsResolved() {
        return limit.resolved() && Resolvables.resolved(order);
    }

    @Override
    protected NodeInfo<TopN> info() {
        return NodeInfo.create(this, (s, c, o, l, loc) -> new TopN(s, c, o, l, loc, unboundedSort), child(), order, limit, local);
    }

    @Override
    public TopN replaceChild(LogicalPlan newChild) {
        return new TopN(source(), newChild, order, limit, local, unboundedSort);
    }

    public TopN withLocal(boolean local) {
        return new TopN(source(), child(), order, limit, local, unboundedSort);
    }

    /**
     * Returns a copy of this TopN with {@code unboundedSort} set to {@code true}.
     * Used by {@code MarkUnboundedSort} to mark the streaming-sort path.
     */
    public TopN withUnboundedSort() {
        return new TopN(source(), child(), order, limit, local, true);
    }

    public boolean local() {
        return local;
    }

    /**
     * Whether this TopN represents an unbounded streaming sort (set by {@code MarkUnboundedSort}).
     */
    public boolean unboundedSort() {
        return unboundedSort;
    }

    public Expression limit() {
        return limit;
    }

    public List<Order> order() {
        return order;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, limit, local, unboundedSort);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            var other = (TopN) obj;
            return Objects.equals(order, other.order)
                && Objects.equals(limit, other.limit)
                && local == other.local
                && unboundedSort == other.unboundedSort;
        }
        return false;
    }

    @Override
    public ExecuteLocation executesOn() {
        return local ? ExecuteLocation.ANY : ExecuteLocation.COORDINATOR;
    }
}
