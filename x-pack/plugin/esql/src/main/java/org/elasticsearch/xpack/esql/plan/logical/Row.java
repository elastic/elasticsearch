/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.session.Executable;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class Row extends LeafPlan implements Executable {

    private final List<Alias> fields;

    public Row(Source source, List<Alias> fields) {
        super(source);
        this.fields = fields;
    }

    public List<Alias> fields() {
        return fields;
    }

    @Override
    public List<Attribute> output() {
        return fields.stream().<Attribute>map(f -> new ReferenceAttribute(f.source(), f.name(), f.dataType())).toList();
    }

    @Override
    public void execute(ActionListener<Result> listener) {
        listener.onResponse(new Result(output(), List.of(fields.stream().map(f -> f.child().fold()).toList())));
    }

    @Override
    public boolean expressionsResolved() {
        return false;
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
