/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Objects;

public class Explain extends LeafPlan implements TelemetryAware {

    public enum Type {
        PARSED,
        ANALYZED
    }

    private final LogicalPlan query;

    public Explain(Source source, LogicalPlan query) {
        super(source);
        this.query = query;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
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
            new ReferenceAttribute(Source.EMPTY, "plan", DataType.KEYWORD),
            new ReferenceAttribute(Source.EMPTY, "type", DataType.KEYWORD)
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
