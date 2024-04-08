/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.Objects;

public class Explain extends LeafPlan {

    public enum Type {
        PARSED,
        ANALYZED
    }

    private final LogicalPlan query;

    public Explain(Source source, LogicalPlan query) {
        super(source);
        this.query = query;
    }

    // TODO: implement again
    // @Override
    // public void execute(EsqlSession session, ActionListener<Result> listener) {
    // ActionListener<String> analyzedStringListener = listener.map(
    // analyzed -> new Result(
    // output(),
    // List.of(List.of(query.toString(), Type.PARSED.toString()), List.of(analyzed, Type.ANALYZED.toString()))
    // )
    // );
    //
    // session.analyzedPlan(
    // query,
    // ActionListener.wrap(
    // analyzed -> analyzedStringListener.onResponse(analyzed.toString()),
    // e -> analyzedStringListener.onResponse(e.toString())
    // )
    // );
    //
    // }

    @Override
    public List<Attribute> output() {
        return List.of(
            new ReferenceAttribute(Source.EMPTY, "plan", DataTypes.KEYWORD),
            new ReferenceAttribute(Source.EMPTY, "type", DataTypes.KEYWORD)
        );
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Explain::new, query);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Explain explain = (Explain) o;
        return Objects.equals(query, explain.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query);
    }
}
