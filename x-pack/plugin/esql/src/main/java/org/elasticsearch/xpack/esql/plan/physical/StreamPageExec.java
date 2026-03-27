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
import org.elasticsearch.xpack.esql.plugin.StreamingProvider;

import java.io.IOException;

/**
 * Physical plan node that replaces {@link OutputExec} when HTTP streaming is enabled.
 * Maps to {@link org.elasticsearch.xpack.esql.plugin.StreamingOutputOperator} in the
 * {@link org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner}.
 * <p>
 * Coordinator-only — not serializable. Holds a runtime reference to
 * {@link StreamingProvider}, similar to how {@link StorePageExec} holds a
 * {@link org.elasticsearch.xpack.esql.plugin.PaginationStoreProvider}.
 */
public class StreamPageExec extends UnaryExec {

    private final StreamingProvider streamingProvider;

    public StreamPageExec(PhysicalPlan child, StreamingProvider streamingProvider) {
        super(null, child);
        this.streamingProvider = streamingProvider;
    }

    public StreamPageExec(Source source, PhysicalPlan child, StreamingProvider streamingProvider) {
        super(source, child);
        this.streamingProvider = streamingProvider;
    }

    public StreamingProvider streamingProvider() {
        return streamingProvider;
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
        return new StreamPageExec(source(), newChild, streamingProvider);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, StreamPageExec::new, child(), streamingProvider);
    }
}
