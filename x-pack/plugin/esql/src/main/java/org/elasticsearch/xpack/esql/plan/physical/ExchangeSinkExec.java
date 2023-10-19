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
import java.util.Objects;

public class ExchangeSinkExec extends UnaryExec {

    private final List<Attribute> output;
    private final boolean intermediateAgg;

    public ExchangeSinkExec(Source source, List<Attribute> output, boolean intermediateAgg, PhysicalPlan child) {
        super(source, child);
        this.output = output;
        this.intermediateAgg = intermediateAgg;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public boolean isIntermediateAgg() {
        return intermediateAgg;
    }

    @Override
    protected NodeInfo<? extends ExchangeSinkExec> info() {
        return NodeInfo.create(this, ExchangeSinkExec::new, output, intermediateAgg, child());
    }

    @Override
    public ExchangeSinkExec replaceChild(PhysicalPlan newChild) {
        return new ExchangeSinkExec(source(), output, intermediateAgg, newChild);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            ExchangeSinkExec that = (ExchangeSinkExec) o;
            return intermediateAgg == that.intermediateAgg && Objects.equals(output, that.output);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, intermediateAgg, child());
    }
}
