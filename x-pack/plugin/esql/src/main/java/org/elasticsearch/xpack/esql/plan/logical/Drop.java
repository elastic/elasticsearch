/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

/**
 * Drop is an intermediary object used during the {@link org.elasticsearch.xpack.esql.analysis.Analyzer} phase of query planning.
 * DROP commands are parsed into Drop objects, which the {@link org.elasticsearch.xpack.esql.analysis.Analyzer.ResolveRefs} rule then
 * rewrites into {@link org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject} plans, along with other projection-like commands.
 * As such, Drop is neither serializable nor able to be mapped to a corresponding physical plan.
 */
public class Drop extends UnaryPlan implements TelemetryAware, SortAgnostic {
    private final List<NamedExpression> removals;

    public Drop(Source source, LogicalPlan child, List<NamedExpression> removals) {
        super(source, child);
        this.removals = removals;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public List<NamedExpression> removals() {
        return removals;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(removals);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Drop(source(), newChild, removals);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Drop::new, child(), removals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), removals);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        return Objects.equals(removals, ((Drop) obj).removals);
    }
}
