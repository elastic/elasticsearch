/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Represents a PromQL function call that produces a scalar value and has no arguments.
 * <p>
 * These functions return a scalar value directly.
 * This corresponds to PromQL syntax:
 * <pre>
 * scalar_function()
 * </pre>
 * Examples: {@code time()}, {@code pi()}
 */
public final class ScalarFunction extends LeafPlan implements PromqlPlan {

    private final String functionName;

    public ScalarFunction(Source source, String functionName) {
        super(source);
        this.functionName = functionName;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, ScalarFunction::new, functionName);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || (obj instanceof ScalarFunction other && Objects.equals(functionName, other.functionName));
    }

    @Override
    public List<Attribute> output() {
        return List.of();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("does not support serialization");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("does not support serialization");
    }

    @Override
    public PromqlDataType returnType() {
        return PromqlDataType.SCALAR;
    }

    public String functionName() {
        return functionName;
    }
}
