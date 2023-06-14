/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class Row extends LeafPlan {

    private final List<NamedExpression> fields;

    public Row(Source source, List<NamedExpression> fields) {
        super(source);
        this.fields = fields;
    }

    public List<NamedExpression> fields() {
        return fields;
    }

    @Override
    public List<Attribute> output() {
        return Expressions.asAttributes(fields);
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(fields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Row::new, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row constant = (Row) o;
        return Objects.equals(fields, constant.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }
}
