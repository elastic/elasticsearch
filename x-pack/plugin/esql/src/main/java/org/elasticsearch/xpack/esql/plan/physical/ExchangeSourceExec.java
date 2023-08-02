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

public class ExchangeSourceExec extends LeafExec {

    private final List<Attribute> output;
    private final boolean intermediateAgg;

    public ExchangeSourceExec(Source source, List<Attribute> output, boolean intermediateAgg) {
        super(source);
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
    protected NodeInfo<ExchangeSourceExec> info() {
        return NodeInfo.create(this, ExchangeSourceExec::new, output, intermediateAgg);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExchangeSourceExec that = (ExchangeSourceExec) o;
        return Objects.equals(output, that.output) && intermediateAgg == that.intermediateAgg;
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, intermediateAgg);
    }
}
