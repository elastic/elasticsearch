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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class LoadResult extends LeafPlan implements TelemetryAware {

    private final Literal searchId;
    private final List<Attribute> output;

    public LoadResult(Source source, Literal searchId) {
        this(source, searchId, Collections.emptyList());
    }

    public LoadResult(Source source, Literal searchId, List<Attribute> output) {
        super(source);
        this.searchId = searchId;
        this.output = output;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public Literal searchId() {
        return searchId;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public LoadResult withOutput(List<Attribute> newOutput) {
        return new LoadResult(source(), searchId, newOutput);
    }

    public boolean resolved() {
        return output.isEmpty() == false;
    }

    @Override
    public boolean expressionsResolved() {
        return searchId.resolved();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, LoadResult::new, searchId, output);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoadResult that = (LoadResult) o;
        return Objects.equals(searchId, that.searchId) && Objects.equals(output, that.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchId, output);
    }
}
