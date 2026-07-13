/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Unresolved PromQL function call produced by the parser and replaced by a resolved node
 * (e.g. {@link WithinSeriesAggregate}, {@link AcrossSeriesAggregate}) during analysis.
 * <p>
 *     The parser cannot validate function names, arity, or parameter types without
 *     consulting the function registry; that semantic work is deliberately deferred
 *     to the analyzer.
 * </p>
 */
public final class UnresolvedPromqlFunction extends LogicalPlan implements PromqlPlan {

    private final String functionName;
    /**
     * All arguments in source order, not yet split into the "child" parameter and
     * the extra literal parameters. Each element is a {@link PromqlPlan} node
     * (a selector, literal selector, or another unresolved / resolved function).
     */
    private final List<LogicalPlan> rawParams;
    /**
     * {@code BY}, {@code WITHOUT}, or {@code null} if no grouping clause appeared in the source.
     */
    @Nullable
    private final AcrossSeriesAggregate.Grouping grouping;
    /**
     * Label attributes from a {@code by(…)}/{@code without(…)} clause; empty when
     * {@code grouping} is {@code null}.
     */
    private final List<Attribute> groupingKeys;

    public UnresolvedPromqlFunction(
        Source source,
        String functionName,
        List<LogicalPlan> rawParams,
        @Nullable AcrossSeriesAggregate.Grouping grouping,
        List<Attribute> groupingKeys
    ) {
        super(source, rawParams);
        this.functionName = functionName;
        this.rawParams = rawParams;
        this.grouping = grouping;
        this.groupingKeys = groupingKeys != null ? groupingKeys : List.of();
    }

    public String functionName() {
        return functionName;
    }

    public List<LogicalPlan> rawParams() {
        return rawParams;
    }

    @Nullable
    public AcrossSeriesAggregate.Grouping grouping() {
        return grouping;
    }

    public List<Attribute> groupingKeys() {
        return groupingKeys;
    }

    @Override
    protected NodeInfo<UnresolvedPromqlFunction> info() {
        return NodeInfo.create(this, UnresolvedPromqlFunction::new, functionName, rawParams, grouping, groupingKeys);
    }

    @Override
    public UnresolvedPromqlFunction replaceChildren(List<LogicalPlan> newChildren) {
        return new UnresolvedPromqlFunction(source(), functionName, newChildren, grouping, groupingKeys);
    }

    @Override
    public List<Attribute> output() {
        return List.of();
    }

    /**
     * Always throws: the return type is not known until the function name has been
     * resolved against the registry during analysis.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public PromqlDataType returnType() {
        throw new UnsupportedOperationException("returnType() called on unresolved PromQL function [" + functionName + "]");
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("UnresolvedPromqlFunction does not support serialization");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("UnresolvedPromqlFunction does not support serialization");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UnresolvedPromqlFunction other = (UnresolvedPromqlFunction) obj;
        return Objects.equals(functionName, other.functionName)
            && Objects.equals(rawParams, other.rawParams)
            && grouping == other.grouping
            && Objects.equals(groupingKeys, other.groupingKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName, rawParams, grouping, groupingKeys);
    }

    @Override
    public String toString() {
        return "UnresolvedPromqlFunction[" + functionName + "]";
    }
}
