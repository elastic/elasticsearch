/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.function.Consumer;

public class OutputExec extends UnaryExec {

    @Nullable
    private final PageConsumer.Factory pageConsumerFactory;

    public OutputExec(PhysicalPlan child, Consumer<Page> pageConsumer) {
        this(null, child, pageConsumer);
    }

    public OutputExec(Source source, PhysicalPlan child, @Nullable Consumer<Page> pageConsumer) {
        this(source, child, pageConsumer == null ? null : PageConsumer.Factory.of(pageConsumer));
    }

    private OutputExec(Source source, PhysicalPlan child, @Nullable PageConsumer.Factory pageConsumerFactory) {
        super(source, child);
        this.pageConsumerFactory = pageConsumerFactory;
    }

    public static OutputExec withFactory(Source source, PhysicalPlan child, PageConsumer.Factory pageConsumerFactory) {
        return new OutputExec(source, child, pageConsumerFactory);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Nullable
    public PageConsumer.Factory getPageConsumerFactory() {
        return pageConsumerFactory;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new OutputExec(source(), newChild, pageConsumerFactory);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, OutputExec::new, child(), pageConsumerFactory);
    }
}
