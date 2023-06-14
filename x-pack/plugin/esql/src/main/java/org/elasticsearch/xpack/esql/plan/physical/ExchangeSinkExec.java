/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class ExchangeSinkExec extends UnaryExec {

    public ExchangeSinkExec(Source source, PhysicalPlan child) {
        super(source, child);
    }

    @Override
    protected NodeInfo<? extends ExchangeSinkExec> info() {
        return NodeInfo.create(this, ExchangeSinkExec::new, child());
    }

    @Override
    public ExchangeSinkExec replaceChild(PhysicalPlan newChild) {
        return new ExchangeSinkExec(source(), newChild);
    }
}
