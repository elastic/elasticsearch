/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.function.Consumer;

public class OutputExec extends UnaryExec {

    private final Consumer<Page> pageConsumer;

    public OutputExec(PhysicalPlan child, Consumer<Page> pageConsumer) {
        super(null, child);
        this.pageConsumer = pageConsumer;
    }

    public OutputExec(Source source, PhysicalPlan child, Consumer<Page> pageConsumer) {
        super(source, child);
        this.pageConsumer = pageConsumer;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public Consumer<Page> getPageConsumer() {
        return pageConsumer;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new OutputExec(source(), newChild, pageConsumer);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, OutputExec::new, child(), pageConsumer);
    }
}
