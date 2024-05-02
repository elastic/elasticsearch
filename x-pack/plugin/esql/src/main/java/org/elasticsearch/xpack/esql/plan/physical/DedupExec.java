/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class DedupExec extends UnaryExec implements EstimatesRowSize {

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final Integer estimatedRowSize;

    private final int limit;

    private final List<Order> order;

    public DedupExec(Source source, PhysicalPlan child, List<Order> order, int limit, Integer estimatedRowSize) {
        super(source, child);
        this.order = order;
        this.limit = limit;
        this.estimatedRowSize = estimatedRowSize;
    }

    @Override
    protected NodeInfo<DedupExec> info() {
        return NodeInfo.create(this, DedupExec::new, child(), order, limit, estimatedRowSize);
    }

    @Override
    public DedupExec replaceChild(PhysicalPlan newChild) {
        return new DedupExec(source(), newChild, order, limit, estimatedRowSize);
    }

    public List<Order> order() {
        return order;
    }

    public int limit() {
        return limit;
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
        return Objects.equals(this.estimatedRowSize, size) ? this : new DedupExec(source(), child(), order, limit, size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, limit, estimatedRowSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DedupExec other = (DedupExec) obj;

        return Objects.equals(order, other.order)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && Objects.equals(child(), other.child());
    }
}
