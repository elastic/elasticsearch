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

public class TopNBy extends UnaryPlan implements PipelineBreaker, ExecutesOn {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "TopNBy", TopNBy::new);

    private final List<Order> order;
    private final Expression limit;
    private List<Expression> groupings;

    /**
     * Local TopNBy is not a pipeline breaker, and is applied only to the local node's data.
     * It should always end up inside a fragment.
     */
    // TODO Do I need this?
    private final transient boolean local;

    public TopNBy(Source source, LogicalPlan child, List<Order> order, Expression limit, List<Expression> groupings, boolean local) {
        super(source, child);
        this.order = order;
        this.limit = limit;
        this.local = local;
        this.groupings = groupings;
    }

    private TopNBy(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readCollectionAsList(Order::new),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            false
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeCollection(order);
        out.writeNamedWriteable(limit);
        out.writeNamedWriteableCollection(groupings);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean expressionsResolved() {
        return limit.resolved() && Resolvables.resolved(order) && Resolvables.resolved(groupings);
    }

    @Override
    protected NodeInfo<TopNBy> info() {
        return NodeInfo.create(this, TopNBy::new, child(), order, limit, groupings, local);
    }

    @Override
    public TopNBy replaceChild(LogicalPlan newChild) {
        return new TopNBy(source(), newChild, order, limit, groupings, local);
    }

    public TopNBy withLocal(boolean local) {
        return new TopNBy(source(), child(), order, limit, groupings, local);
    }

    public boolean local() {
        return local;
    }

    public Expression limit() {
        return limit;
    }

    public List<Order> order() {
        return order;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, limit, groupings, local);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            var other = (TopNBy) obj;
            return Objects.equals(order, other.order)
                && Objects.equals(limit, other.limit)
                && Objects.equals(groupings, other.groupings)
                && local == other.local;
        }
        return false;
    }

    @Override
    public ExecuteLocation executesOn() {
        return local ? ExecuteLocation.ANY : ExecuteLocation.COORDINATOR;
    }
}
