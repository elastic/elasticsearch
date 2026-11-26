/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

/**
 * A physical plan node that routes pages to a {@link PageStreamPublisher} for HTTP streaming.
 * Like {@link OutputExec}, this node is coordinator-only and is never serialized to data nodes.
 */
public class StreamingOutputExec extends UnaryExec {

    private final PageStreamPublisher pageStream;

    public StreamingOutputExec(PhysicalPlan child, PageStreamPublisher pageStream) {
        this(null, child, pageStream);
    }

    public StreamingOutputExec(Source source, PhysicalPlan child, PageStreamPublisher pageStream) {
        super(source, child);
        this.pageStream = pageStream;
    }

    public PageStreamPublisher pageStream() {
        return pageStream;
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
        return new StreamingOutputExec(source(), newChild, pageStream);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, StreamingOutputExec::new, child(), pageStream);
    }
}
