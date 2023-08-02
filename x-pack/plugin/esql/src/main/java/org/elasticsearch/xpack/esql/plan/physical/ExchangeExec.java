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

import static java.util.Collections.emptyList;

public class ExchangeExec extends UnaryExec {

    private final List<Attribute> output;
    private final boolean inBetweenAggs;

    public ExchangeExec(Source source, PhysicalPlan child) {
        this(source, emptyList(), false, child);
    }

    public ExchangeExec(Source source, List<Attribute> output, boolean inBetweenAggs, PhysicalPlan child) {
        super(source, child);
        this.output = output;
        this.inBetweenAggs = inBetweenAggs;
    }

    @Override
    public List<Attribute> output() {
        return output.isEmpty() ? super.output() : output;
    }

    public boolean isInBetweenAggs() {
        return inBetweenAggs;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new ExchangeExec(source(), output, inBetweenAggs, newChild);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ExchangeExec::new, output, inBetweenAggs, child());
    }
}
