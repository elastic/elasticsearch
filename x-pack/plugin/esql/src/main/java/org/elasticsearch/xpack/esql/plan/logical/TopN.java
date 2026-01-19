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
    protected final List<Expression> groupings;

    /**
     * Local topn is not a pipeline breaker, and is applied only to the local node's data.
     * It should always end up inside a fragment.
     */
    private final transient boolean local;

    public TopN(Source source, LogicalPlan child, List<Order> order, Expression limit, List<Expression> groupings, boolean local) {
        super(source, child);
        this.order = order;
        this.limit = limit;
        this.local = local;
        this.groupings = groupings;
    }

    private TopN(StreamInput in) throws IOException {
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
    protected NodeInfo<TopN> info() {
        return NodeInfo.create(this, TopN::new, child(), order, limit, groupings, local);
    }

    @Override
    public TopN replaceChild(LogicalPlan newChild) {
        return new TopN(source(), newChild, order, limit, groupings, local);
    }

    public TopN withLocal(boolean local) {
        return new TopN(source(), child(), order, limit, groupings, local);
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

    /**
     * What this aggregation is grouped by. Generally, this corresponds to the {@code BY} clause, even though this command will not output
     * those values unless they are also part of the {@link Aggregate#aggregates()}. This enables grouping without outputting the grouping
     * keys, and makes it so that an {@link Aggregate}s also acts as a projection.
     * <p>
     * The actual grouping keys will be extracted from multivalues, so that if the grouping is on {@code mv_field}, and the document has
     * {@code mv_field: [1, 2, 2]}, then the document will be part of the groups for both {@code mv_field=1} and {@code mv_field=2} (and
     * counted only once in each group).
     */
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
            var other = (TopN) obj;
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
