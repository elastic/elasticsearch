/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class FilterExec extends UnaryExec {

    private final Expression condition;

    public FilterExec(Source source, PhysicalPlan child, Expression condition) {
        super(source, child);
        this.condition = condition;
    }

    @Override
    protected NodeInfo<FilterExec> info() {
        return NodeInfo.create(this, FilterExec::new, child(), condition);
    }

    @Override
    public FilterExec replaceChild(PhysicalPlan newChild) {
        return new FilterExec(source(), newChild, condition);
    }

    public Expression condition() {
        return condition;
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, child());
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
        return Objects.equals(condition, other.condition) && Objects.equals(child(), other.child());
    }
}
