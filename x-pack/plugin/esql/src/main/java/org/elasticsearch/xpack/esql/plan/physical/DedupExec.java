/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class DedupExec extends UnaryExec {

    private final List<? extends NamedExpression> fields;

    public DedupExec(Source source, PhysicalPlan child, List<? extends NamedExpression> fields) {
        super(source, child);
        this.fields = fields;
    }

    @Override
    protected NodeInfo<DedupExec> info() {
        return NodeInfo.create(this, DedupExec::new, child(), fields);
    }

    @Override
    public DedupExec replaceChild(PhysicalPlan newChild) {
        return new DedupExec(source(), newChild, fields);
    }

    public List<? extends NamedExpression> fields() {
        return fields;
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

        DedupExec other = (DedupExec) obj;

        return Objects.equals(fields, other.fields) && Objects.equals(child(), other.child());
    }
}
