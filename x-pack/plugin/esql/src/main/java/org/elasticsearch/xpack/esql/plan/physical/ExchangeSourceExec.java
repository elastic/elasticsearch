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
    private final PhysicalPlan planUsedForLayout;

    public ExchangeSourceExec(Source source, List<Attribute> output, PhysicalPlan fragmentPlanUsedForLayout) {
        super(source);
        this.output = output;
        this.planUsedForLayout = fragmentPlanUsedForLayout;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public PhysicalPlan nodeLayout() {
        return planUsedForLayout;
    }

    @Override
    protected NodeInfo<ExchangeSourceExec> info() {
        return NodeInfo.create(this, ExchangeSourceExec::new, output, planUsedForLayout);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExchangeSourceExec that = (ExchangeSourceExec) o;
        return Objects.equals(output, that.output) && Objects.equals(planUsedForLayout, that.planUsedForLayout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, planUsedForLayout);
    }
}
