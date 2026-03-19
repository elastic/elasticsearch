/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plugin.PaginationStoreProvider;

import java.io.IOException;

/**
 * Physical plan node that replaces {@link OutputExec} when pagination is enabled.
 * Maps to {@link org.elasticsearch.xpack.esql.plugin.StorePageOperator} in the
 * {@link org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner}.
 * <p>
 * Coordinator-only — not serializable. Holds a runtime reference to
 * {@link PaginationStoreProvider}, similar to how {@link OutputExec} holds a
 * {@code Consumer<Page>}.
 */
public class StorePageExec extends UnaryExec {

    private final PaginationStoreProvider storeProvider;

    public StorePageExec(PhysicalPlan child, PaginationStoreProvider storeProvider) {
        super(null, child);
        this.storeProvider = storeProvider;
    }

    public StorePageExec(Source source, PhysicalPlan child, PaginationStoreProvider storeProvider) {
        super(source, child);
        this.storeProvider = storeProvider;
    }

    public PaginationStoreProvider storeProvider() {
        return storeProvider;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new StorePageExec(source(), newChild, storeProvider);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, StorePageExec::new, child(), storeProvider);
    }
}
