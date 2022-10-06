/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.action.compute.data.Page;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.function.BiConsumer;

public class Output extends UnaryPlan {

    private final BiConsumer<List<String>, Page> pageConsumer;

    public Output(LogicalPlan child, BiConsumer<List<String>, Page> pageConsumer) {
        super(null, child);
        this.pageConsumer = pageConsumer;
    }

    public Output(Source source, LogicalPlan child, BiConsumer<List<String>, Page> pageConsumer) {
        super(source, child);
        this.pageConsumer = pageConsumer;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    public BiConsumer<List<String>, Page> getPageConsumer() {
        return pageConsumer;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Output(source(), newChild, pageConsumer);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Output::new, child(), pageConsumer);
    }
}
