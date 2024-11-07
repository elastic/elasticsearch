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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TopNExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "TopNExec",
        TopNExec::new
    );

    private final Expression limit;
    private final List<Order> order;

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final Integer estimatedRowSize;

    public TopNExec(Source source, PhysicalPlan child, List<Order> order, Expression limit, Integer estimatedRowSize) {
        super(source, child);
        this.order = order;
        this.limit = limit;
        this.estimatedRowSize = estimatedRowSize;
    }

    private TopNExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readCollectionAsList(org.elasticsearch.xpack.esql.expression.Order::new),
            in.readNamedWriteable(Expression.class),
            in.readOptionalVInt()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeCollection(order());
        out.writeNamedWriteable(limit());
        out.writeOptionalVInt(estimatedRowSize());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<TopNExec> info() {
        return NodeInfo.create(this, TopNExec::new, child(), order, limit, estimatedRowSize);
    }

    @Override
    public TopNExec replaceChild(PhysicalPlan newChild) {
        return new TopNExec(source(), newChild, order, limit, estimatedRowSize);
    }

    public Expression limit() {
        return limit;
    }

    public List<Order> order() {
        return order;
    }

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        int size = state.consumeAllFields(true);
        return Objects.equals(this.estimatedRowSize, size) ? this : new TopNExec(source(), child(), order, limit, size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, limit, estimatedRowSize);
    }

    @Override
    public boolean equals(Object obj) {
        boolean equals = super.equals(obj);
        if (equals) {
            var other = (TopNExec) obj;
            equals = Objects.equals(order, other.order)
                && Objects.equals(limit, other.limit)
                && Objects.equals(estimatedRowSize, other.estimatedRowSize);
        }
        return equals;
    }
}
