/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class CompositeAggExec extends UnaryExec implements Unexecutable {

    private final List<? extends NamedExpression> keys;

    public CompositeAggExec(Source source, PhysicalPlan child, List<? extends NamedExpression> keys) {
        super(source, child);
        this.keys = keys;
    }

    @Override
    protected NodeInfo<CompositeAggExec> info() {
        return NodeInfo.create(this, CompositeAggExec::new, child(), keys);
    }

    @Override
    protected CompositeAggExec replaceChild(PhysicalPlan newChild) {
        return new CompositeAggExec(source(), newChild, keys);
    }

    public List<? extends NamedExpression> keys() {
        return keys;
    }

    @Override
    public int hashCode() {
        return Objects.hash(keys, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CompositeAggExec other = (CompositeAggExec) obj;

        return Objects.equals(keys, other.keys) && Objects.equals(child(), other.child());
    }
}
