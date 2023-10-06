/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class ExchangeSinkExec extends UnaryExec {

    private final List<Attribute> output;

    public ExchangeSinkExec(Source source, List<Attribute> output, PhysicalPlan child) {
        super(source, child);
        this.output = output;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected NodeInfo<? extends ExchangeSinkExec> info() {
        return NodeInfo.create(this, ExchangeSinkExec::new, output, child());
    }

    @Override
    public ExchangeSinkExec replaceChild(PhysicalPlan newChild) {
        return new ExchangeSinkExec(source(), output, newChild);
    }
}
