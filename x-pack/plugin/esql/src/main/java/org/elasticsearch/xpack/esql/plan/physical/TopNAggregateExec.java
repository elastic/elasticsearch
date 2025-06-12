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
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TopNAggregateExec extends AbstractAggregateExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "TopNAggregateExec",
        TopNAggregateExec::new
    );

    private final List<Order> order;
    private final Expression limit;

    public TopNAggregateExec(
        Source source,
        PhysicalPlan child,
        List<? extends Expression> groupings,
        List<? extends NamedExpression> aggregates,
        AggregatorMode mode,
        List<Attribute> intermediateAttributes,
        Integer estimatedRowSize,
        List<Order> order,
        Expression limit
    ) {
        super(source, child, groupings, aggregates, mode, intermediateAttributes, estimatedRowSize);
        this.order = order;
        this.limit = limit;
    }

    protected TopNAggregateExec(StreamInput in) throws IOException {
        // This is only deserialized as part of node level reduction, which is turned off until at least 8.16.
        // So, we do not have to consider previous transport versions here, because old nodes will not send AggregateExecs to new nodes.
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
    protected NodeInfo<TopNAggregateExec> info() {
        return NodeInfo.create(
            this,
            TopNAggregateExec::new,
            child(),
            groupings,
            aggregates,
            mode,
            intermediateAttributes,
            estimatedRowSize,
            order,
            limit
        );
    }

    @Override
    public TopNAggregateExec replaceChild(PhysicalPlan newChild) {
        return new TopNAggregateExec(
            source(),
            newChild,
            groupings,
            aggregates,
            mode,
            intermediateAttributes,
            estimatedRowSize,
            order,
            limit
        );
    }

    public List<Order> order() {
        return order;
    }

    public Expression limit() {
        return limit;
    }

    @Override
    public TopNAggregateExec withAggregates(List<? extends NamedExpression> newAggregates) {
        return new TopNAggregateExec(
            source(),
            child(),
            groupings,
            newAggregates,
            mode,
            intermediateAttributes,
            estimatedRowSize,
            order,
            limit
        );
    }

    @Override
    public TopNAggregateExec withMode(AggregatorMode newMode) {
        return new TopNAggregateExec(
            source(),
            child(),
            groupings,
            aggregates,
            newMode,
            intermediateAttributes,
            estimatedRowSize,
            order,
            limit
        );
    }

    @Override
    protected TopNAggregateExec withEstimatedSize(int estimatedRowSize) {
        return new TopNAggregateExec(
            source(),
            child(),
            groupings,
            aggregates,
            mode,
            intermediateAttributes,
            estimatedRowSize,
            order,
            limit
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, mode, intermediateAttributes, estimatedRowSize, order, limit, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TopNAggregateExec other = (TopNAggregateExec) obj;
        return Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(mode, other.mode)
            && Objects.equals(intermediateAttributes, other.intermediateAttributes)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && Objects.equals(order, other.order)
            && Objects.equals(limit, other.limit)
            && Objects.equals(child(), other.child());
    }

}
