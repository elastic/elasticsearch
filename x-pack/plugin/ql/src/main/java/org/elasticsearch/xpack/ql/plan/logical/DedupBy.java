/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class DedupBy extends UnaryPlan {

    private final List<? extends NamedExpression> fields;

    public DedupBy(Source source, LogicalPlan child, List<? extends NamedExpression> fields) {
        super(source, child);
        this.fields = fields;
    }

    @Override
    protected NodeInfo<DedupBy> info() {
        return NodeInfo.create(this, DedupBy::new, child(), fields);
    }

    @Override
    public DedupBy replaceChild(LogicalPlan newChild) {
        return new DedupBy(source(), newChild, fields);
    }

    public List<? extends NamedExpression> fields() {
        return fields;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DedupBy other = (DedupBy) obj;
        return Objects.equals(fields, other.fields) && Objects.equals(child(), other.child());
    }
}
