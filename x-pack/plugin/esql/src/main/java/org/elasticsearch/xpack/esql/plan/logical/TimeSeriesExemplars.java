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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.util.List;

/**
 * Parser output of {@code TS_EXEMPLARS (<metrics query>)}: holds the metrics query whose exemplars are to be fetched. The exemplar
 * data streams are not known at parse time; they are derived from the metrics data streams the metrics query actually matches once
 * its index pattern is resolved. {@link org.elasticsearch.xpack.esql.session.TimeSeriesExemplarsRewriter} then plans (but never
 * executes) the metrics query and replaces this node with a query over those exemplar data streams before the plan is analyzed, so it
 * is never serialized or executed.
 */
public class TimeSeriesExemplars extends UnaryPlan implements TelemetryAware {

    public TimeSeriesExemplars(Source source, LogicalPlan metricsQuery) {
        super(source, metricsQuery);
    }

    public LogicalPlan metricsQuery() {
        return child();
    }

    /**
     * The index pattern the metrics query reads; both {@code TS} and {@code PROMQL} contribute exactly one relation.
     */
    public IndexPattern metricsIndexPattern() {
        return metricsQuery().collect(UnresolvedRelation.class).getFirst().indexPattern();
    }

    @Override
    public List<Attribute> output() {
        return List.of();
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public TimeSeriesExemplars replaceChild(LogicalPlan newChild) {
        return new TimeSeriesExemplars(source(), newChild);
    }

    @Override
    protected NodeInfo<TimeSeriesExemplars> info() {
        return NodeInfo.create(this, TimeSeriesExemplars::new, child());
    }

    @Override
    public String telemetryLabel() {
        return "TS_EXEMPLARS";
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }
}
