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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TopNAggregate extends Aggregate {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "TopNAggregate",
        TopNAggregate::new
    );

    private final List<Order> order;
    private final Expression limit;

    protected List<Attribute> lazyOutput;

    public TopNAggregate(
        Source source,
        LogicalPlan child,
        List<Expression> groupings,
        List<? extends NamedExpression> aggregates,
        List<Order> order,
        Expression limit
    ) {
        super(source, child, groupings, aggregates);
        this.order = order;
        this.limit = limit;
    }

    public TopNAggregate(StreamInput in) throws IOException {
        super(in);
        this.order = in.readCollectionAsList(Order::new);
        this.limit = in.readNamedWriteable(Expression.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(order);
        out.writeNamedWriteable(limit);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends TopNAggregate> info() {
        return NodeInfo.create(this, TopNAggregate::new, child(), groupings, aggregates, order, limit);
    }

    @Override
    public TopNAggregate replaceChild(LogicalPlan newChild) {
        return new TopNAggregate(source(), newChild, groupings, aggregates, order, limit);
    }

    public List<Order> order() {
        return order;
    }

    public Expression limit() {
        return limit;
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && Resolvables.resolved(order) && limit.resolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, order, limit, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TopNAggregate other = (TopNAggregate) obj;
        return Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(order, other.order)
            && Objects.equals(limit, other.limit)
            && Objects.equals(child(), other.child());
    }
}
