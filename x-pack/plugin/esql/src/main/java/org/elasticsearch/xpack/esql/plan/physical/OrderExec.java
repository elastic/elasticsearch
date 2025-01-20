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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class OrderExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "OrderExec",
        OrderExec::new
    );

    private final List<Order> order;

    public OrderExec(Source source, PhysicalPlan child, List<Order> order) {
        super(source, child);
        this.order = order;
    }

    private OrderExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readCollectionAsList(org.elasticsearch.xpack.esql.expression.Order::new)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeCollection(order());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<OrderExec> info() {
        return NodeInfo.create(this, OrderExec::new, child(), order);
    }

    @Override
    public OrderExec replaceChild(PhysicalPlan newChild) {
        return new OrderExec(source(), newChild, order);
    }

    public List<Order> order() {
        return order;
    }

    @Override
    public int hashCode() {
        return Objects.hash(order, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        OrderExec other = (OrderExec) obj;

        return Objects.equals(order, other.order) && Objects.equals(child(), other.child());
    }
}
