/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

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

    public Consumer<Page> getPageConsumer() {
        return pageConsumer;
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
