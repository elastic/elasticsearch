/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class FilterExec extends UnaryExec implements Unexecutable {

    private final Expression condition;
    private final boolean onAggs;

    public FilterExec(Source source, PhysicalPlan child, Expression condition) {
        this(source, child, condition, false);
    }

    public FilterExec(Source source, PhysicalPlan child, Expression condition, boolean onAggs) {
        super(source, child);
        this.condition = condition;
        this.onAggs = onAggs;
    }

    @Override
    protected NodeInfo<FilterExec> info() {
        return NodeInfo.create(this, FilterExec::new, child(), condition, onAggs);
    }

    @Override
    protected FilterExec replaceChild(PhysicalPlan newChild) {
        return new FilterExec(source(), newChild, condition, onAggs);
    }

    public Expression condition() {
        return condition;
    }

    public boolean onAggs() {
        return onAggs;
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, onAggs, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FilterExec other = (FilterExec) obj;
        return onAggs == other.onAggs && Objects.equals(condition, other.condition) && Objects.equals(child(), other.child());
    }
}
