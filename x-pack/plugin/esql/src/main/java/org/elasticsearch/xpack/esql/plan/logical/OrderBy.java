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
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class OrderBy extends UnaryPlan
    implements
        PostAnalysisVerificationAware,
        PostOptimizationVerificationAware,
        TelemetryAware,
        SortAgnostic {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "OrderBy", OrderBy::new);

    private final List<Order> order;

    public OrderBy(Source source, LogicalPlan child, List<Order> order) {
        super(source, child);
        this.order = order;
    }

    private OrderBy(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
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
    protected NodeInfo<OrderBy> info() {
        return NodeInfo.create(this, OrderBy::new, child(), order);
    }

    @Override
    public OrderBy replaceChild(LogicalPlan newChild) {
        return new OrderBy(source(), newChild, order);
    }

    public List<Order> order() {
        return order;
    }

    @Override
    public String telemetryLabel() {
        return "SORT";
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(order);
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

        OrderBy other = (OrderBy) obj;
        return Objects.equals(order, other.order) && Objects.equals(child(), other.child());
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        /**
         * Some datatypes are not sortable
         */
        order.forEach(order -> {
            if (DataType.isSortable(order.dataType()) == false) {
                failures.add(fail(order, "cannot sort on " + order.dataType().typeName()));
            }
        });
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        failures.add(fail(this, "Unbounded sort not supported yet [{}] please add a limit", this.sourceText()));
    }
}
