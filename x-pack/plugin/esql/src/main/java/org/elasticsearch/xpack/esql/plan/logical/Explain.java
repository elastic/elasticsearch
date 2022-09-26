/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.Executable;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.Objects;

public class Explain extends LeafPlan implements Executable {

    public enum Type {
        PARSED
    }

    private final LogicalPlan query;

    public Explain(Source source, LogicalPlan query) {
        super(source);
        this.query = query;
    }

    @Override
    public void execute(EsqlSession session, ActionListener<Result> listener) {
        listener.onResponse(new Result(output(), List.of(List.of(query.toString(), Type.PARSED.toString()))));
    }

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
