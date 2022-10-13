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

import java.util.List;
import java.util.function.BiConsumer;

public class OutputExec extends UnaryExec {

    private final BiConsumer<List<String>, Page> pageConsumer;

    public OutputExec(PhysicalPlan child, BiConsumer<List<String>, Page> pageConsumer) {
        super(null, child);
        this.pageConsumer = pageConsumer;
    }

    public OutputExec(Source source, PhysicalPlan child, BiConsumer<List<String>, Page> pageConsumer) {
        super(source, child);
        this.pageConsumer = pageConsumer;
    }

    public BiConsumer<List<String>, Page> getPageConsumer() {
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
