/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

@Experimental
public class ExchangeExec extends UnaryExec {

    public ExchangeExec(Source source, PhysicalPlan child) {
        super(source, child);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new ExchangeExec(source(), newChild);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ExchangeExec::new, child());
    }
}
