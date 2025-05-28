/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

/**
 * A physical plan node that hints the plan should be partitioned vertically and executed in parallel.
 */
public final class ParallelExec extends UnaryExec {

    public ParallelExec(Source source, PhysicalPlan child) {
        super(source, child);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("local plan");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("local plan");
    }

    @Override
    protected NodeInfo<? extends ParallelExec> info() {
        return NodeInfo.create(this, ParallelExec::new, child());
    }

    @Override
    public ParallelExec replaceChild(PhysicalPlan newChild) {
        return new ParallelExec(source(), newChild);
    }
}
