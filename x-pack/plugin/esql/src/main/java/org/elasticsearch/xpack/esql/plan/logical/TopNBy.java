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

/**
 * Logical plan node for {@code SORT order1, order2 | LIMIT N BY grouping1, grouping2, ...}.
 * Sorts the input rows retaining at most N rows per group defined by the grouping expressions.
 */
public class TopNBy extends UnaryPlan implements PipelineBreaker {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "TopNBy", TopNBy::new);

    private final List<Order> order;
    private final Expression limitPerGroup;
    private final List<Expression> groupings;

    public TopNBy(Source source, LogicalPlan child, List<Order> order, Expression limitPerGroup, List<Expression> groupings) {
        super(source, child);
        this.order = order;
        this.limitPerGroup = limitPerGroup;
        this.groupings = groupings;
    }

    private TopNBy(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readCollectionAsList(Order::new),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeCollection(order);
        out.writeNamedWriteable(limitPerGroup);
        out.writeNamedWriteableCollection(groupings);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean expressionsResolved() {
        return limitPerGroup.resolved() && Resolvables.resolved(order) && Resolvables.resolved(groupings);
    }

    @Override
    protected NodeInfo<TopNBy> info() {
        return NodeInfo.create(this, TopNBy::new, child(), order, limitPerGroup, groupings);
    }

    @Override
    public TopNBy replaceChild(LogicalPlan newChild) {
        return new TopNBy(source(), newChild, order, limitPerGroup, groupings);
    }

    public Expression limitPerGroup() {
        return limitPerGroup;
    }

    public List<Order> order() {
        return order;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, limitPerGroup, groupings);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            var other = (TopNBy) obj;
            return Objects.equals(order, other.order)
                && Objects.equals(limitPerGroup, other.limitPerGroup)
                && Objects.equals(groupings, other.groupings);
        }
        return false;
    }
}
