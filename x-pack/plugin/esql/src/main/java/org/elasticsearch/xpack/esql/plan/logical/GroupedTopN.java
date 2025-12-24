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

public class GroupedTopN extends UnaryPlan implements PipelineBreaker, ExecutesOn {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "TopN", GroupedTopN::new);

    private final List<Order> order;
    private final Expression limit;
    private final Expression groupKey;
    /**
     * Local topn is not a pipeline breaker, and is applied only to the local node's data.
     * It should always end up inside a fragment.
     */
    private final transient boolean local;

    public GroupedTopN(Source source, LogicalPlan child, List<Order> order, Expression limit, Expression groupKey, boolean local) {
        super(source, child);
        this.order = order;
        this.limit = limit;
        this.local = local;
        this.groupKey = groupKey;
    }

    private GroupedTopN(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readCollectionAsList(Order::new),
            in.readNamedWriteable(Expression.class),
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
        out.writeNamedWriteable(groupKey);
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
    protected NodeInfo<GroupedTopN> info() {
        return NodeInfo.create(this, GroupedTopN::new, child(), order, limit, groupKey, local);
    }

    @Override
    public GroupedTopN replaceChild(LogicalPlan newChild) {
        return new GroupedTopN(source(), newChild, order, limit, groupKey, local);
    }

    public GroupedTopN withLocal(boolean local) {
        return new GroupedTopN(source(), child(), order, limit, groupKey, local);
    }

    public boolean local() {
        return local;
    }

    public Expression limit() {
        return limit;
    }

    public Expression groupKey() {
        return groupKey;
    }

    public List<Order> order() {
        return order;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, limit, groupKey, local);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            var other = (GroupedTopN) obj;
            return Objects.equals(order, other.order)
                && Objects.equals(limit, other.limit)
                && Objects.equals(groupKey, other.groupKey)
                && local == other.local;
        }
        return false;
    }

    @Override
    public ExecuteLocation executesOn() {
        return local ? ExecuteLocation.ANY : ExecuteLocation.COORDINATOR;
    }
}
