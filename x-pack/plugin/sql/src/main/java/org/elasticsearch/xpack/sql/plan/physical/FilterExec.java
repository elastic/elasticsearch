/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import java.util.List;
import java.util.Objects;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class FilterExec extends UnaryExec implements Unexecutable {

    private final Expression condition;
    // indicates whether the filter is regular or agg-based (HAVING xxx)
    // gets setup automatically and then copied over during cloning
    private final boolean isHaving;

    public FilterExec(Source source, PhysicalPlan child, Expression condition) {
        this(source, child, condition, child instanceof AggregateExec);
    }

    public FilterExec(Source source, PhysicalPlan child, Expression condition, boolean isHaving) {
        super(source, child);
        this.condition = condition;
        this.isHaving = isHaving;
    }

    @Override
    protected NodeInfo<FilterExec> info() {
        return NodeInfo.create(this, FilterExec::new, child(), condition, isHaving);
    }

    @Override
    protected FilterExec replaceChild(PhysicalPlan newChild) {
        return new FilterExec(source(), newChild, condition, isHaving);
    }

    public Expression condition() {
        return condition;
    }

    public boolean isHaving() {
        return isHaving;
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, isHaving, child());
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
        return Objects.equals(condition, other.condition)
                && Objects.equals(child(), other.child());
    }
}
